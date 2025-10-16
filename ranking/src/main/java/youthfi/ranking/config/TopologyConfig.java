package youthfi.ranking.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import youthfi.ranking.model.ExecutionRow;
import youthfi.ranking.model.PortfolioAgg;
import youthfi.ranking.model.RankItem;
import youthfi.ranking.model.UserStockRow;
import youthfi.ranking.transformer.TopNTransformer;
import youthfi.ranking.util.DebeziumParser;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Configuration
public class TopologyConfig {

    private static final ObjectMapper M = new ObjectMapper()
            .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);

    @Value("${topics.userstock}") String userStockTopic;
    @Value("${topics.execution}") String executionTopic;
    @Value("${topics.out}") String outTopic;

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {

        // 1) userstock: KTable (key = userId|stockId)
        KTable<String, UserStockRow> userStock = builder
                .stream(userStockTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(DebeziumParser::parseUserStock)
                .filter((k,v) -> v != null)
                .selectKey((k,v) -> v.getUserId() + "|" + v.getStockId())
                .toTable(Materialized.<String, UserStockRow, KeyValueStore<Bytes, byte[]>>as("userstock-table")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(userStockSerde()));

        // 2) execution: KTable (key = userId|stockId, value = ExecutionRow)
        KTable<String, ExecutionRow> executionTable = builder
                .stream(executionTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(DebeziumParser::parseExecution)
                .filter((k,v) -> v != null)
                .selectKey((k,v) -> v.getUserId() + "|" + v.getStockId())
                .toTable(Materialized.<String, ExecutionRow, KeyValueStore<Bytes, byte[]>>as("execution-table")
                        .withKeySerde(Serdes.String())
                        .withValueSerde(executionSerde()));

        // 3) 종목 단위 포지션 합성
        KTable<String, PortfolioAgg> positions = userStock.leftJoin(
                executionTable,
                (row, exec) -> {
                    double currentPrice = (exec == null) ? row.getAvgPrice() : exec.getPrice();
                    double invested = row.getAvgPrice() * row.getHoldingQuantity();
                    double current  = currentPrice        * row.getHoldingQuantity();
                    return new PortfolioAgg(invested, current);
                }
        );

        // 4) 유저 단위 합산
        Serde<PortfolioAgg> aggSerde = portfolioSerde();
        KTable<String, PortfolioAgg> userAgg = positions
                .groupBy(
                        (userIdStockId, agg) -> KeyValue.pair(userIdStockId.split("\\|", 2)[0], agg),
                        Grouped.with(Serdes.String(), aggSerde)
                )
                .reduce(
                        // add
                        (oldV, newV) -> new PortfolioAgg(
                                (oldV==null?0:oldV.getInvested()) + newV.getInvested(),
                                (oldV==null?0:oldV.getCurrent())  + newV.getCurrent()
                        ),
                        (oldV, newV) -> new PortfolioAgg(
                                (oldV==null?0:oldV.getInvested()) - newV.getInvested(),
                                (oldV==null?0:oldV.getCurrent())  - newV.getCurrent()
                        ),
                        Materialized.<String, PortfolioAgg, KeyValueStore<Bytes, byte[]>>as("user-agg-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggSerde)
                );

        // 5) 활동 유저 KTable: user_id별 보유수량 합 > 0
        KTable<String, Integer> qtyByUser = userStock
                .mapValues(UserStockRow::getHoldingQuantity)
                .groupBy(
                        (key, qty) -> KeyValue.pair(key.split("\\|", 2)[0], qty),
                        Grouped.with(Serdes.String(), Serdes.Integer())
                )
                .reduce(
                        (agg, nv) -> (agg==null?0:agg) + (nv==null?0:nv),
                        (agg, ov) -> (agg==null?0:agg) - (ov==null?0:ov),
                        Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as("qty-by-user")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Integer())
                );

        // sum(qty) > 0 인 유저만 유지 (null은 삭제)
        KTable<String, String> activeUsers = qtyByUser
                .mapValues(sum -> (sum != null && sum > 0) ? "1" : null)
                .filter((u, flag) -> flag != null,
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("active-users")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );

        // 6) 수익률 스트림 (0% 허용, NaN/Inf 방지)
        KStream<String, Double> userProfitRate = userAgg
                .toStream()
                .mapValues(PortfolioAgg::profitRatePct) // invested<=0 => 0.0
                .mapValues(r -> (r==null || Double.isNaN(r) || Double.isInfinite(r)) ? 0.0 : r);

        // 7) 활동 유저와 INNER JOIN → 무보유/유령 제거
        KStream<String, Double> filteredRate = userProfitRate.join(
                activeUsers,
                (rate, _flag) -> rate,
                Joined.with(Serdes.String(), Serdes.Double(), Serdes.String())
        );

        // 8) Top10 계산 (tombstone 대응 Transformer)
        var storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("top10-store"),
                Serdes.String(), Serdes.Double());
        builder.addStateStore(storeBuilder);

        KStream<String, List<RankItem>> top10 = filteredRate
                .transformValues(() -> new TopNTransformer("top10-store"), "top10-store");

        // 9) snake_case JSON으로 발행
        top10
                .mapValues(list -> {
                    try { return M.writeValueAsString(list); }
                    catch (Exception e) { return "[]"; }
                })
                .selectKey((k,v) -> "TOP10")
                .to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.stream(outTopic, Consumed.with(Serdes.String(), Serdes.String()));
    }

    // ---- Serde helpers ----
    private Serde<UserStockRow> userStockSerde() {
        var ser = new org.apache.kafka.common.serialization.Serializer<UserStockRow>() {
            @Override public byte[] serialize(String topic, UserStockRow d) {
                if (d == null) return null;
                try { return M.writeValueAsBytes(d); }
                catch (Exception e) { throw new RuntimeException(e); }
            }
        };
        var de = new org.apache.kafka.common.serialization.Deserializer<UserStockRow>() {
            @Override public UserStockRow deserialize(String topic, byte[] bytes) {
                if (bytes == null) return null;
                try { return M.readValue(bytes, UserStockRow.class); }
                catch (Exception e) { throw new RuntimeException(e); }
            }
        };
        return Serdes.serdeFrom(ser, de);
    }

    private Serde<PortfolioAgg> portfolioSerde() {
        var ser = new org.apache.kafka.common.serialization.Serializer<PortfolioAgg>() {
            @Override public byte[] serialize(String topic, PortfolioAgg d) {
                if (d == null) return null;
                String s = d.getInvested() + "," + d.getCurrent();
                return s.getBytes(StandardCharsets.UTF_8);
            }
        };
        var de = new org.apache.kafka.common.serialization.Deserializer<PortfolioAgg>() {
            @Override public PortfolioAgg deserialize(String topic, byte[] bytes) {
                if (bytes == null) return null;
                String[] p = new String(bytes, StandardCharsets.UTF_8).split(",");
                return new PortfolioAgg(Double.parseDouble(p[0]), Double.parseDouble(p[1]));
            }
        };
        return Serdes.serdeFrom(ser, de);
    }

    private Serde<ExecutionRow> executionSerde() {
        var ser = new org.apache.kafka.common.serialization.Serializer<ExecutionRow>() {
            @Override public byte[] serialize(String topic, ExecutionRow d) {
                if (d == null) return null;
                String s = d.getUserId() + "," + d.getStockId() + "," + d.getPrice();
                return s.getBytes(StandardCharsets.UTF_8);
            }
        };
        var de = new org.apache.kafka.common.serialization.Deserializer<ExecutionRow>() {
            @Override public ExecutionRow deserialize(String topic, byte[] bytes) {
                if (bytes == null) return null;
                String[] p = new String(bytes, StandardCharsets.UTF_8).split(",");
                return new ExecutionRow(p[0], p[1], Double.parseDouble(p[2]));
            }
        };
        return Serdes.serdeFrom(ser, de);
    }
}