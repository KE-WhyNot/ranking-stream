package youthfi.ranking.config;

import com.fasterxml.jackson.databind.ObjectMapper;
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

    private static final ObjectMapper M = new ObjectMapper();

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

        // 3) KTable-KTable 조인: userstock과 execution을 userId|stockId로 조인
        KTable<String, PortfolioAgg> positions = userStock.leftJoin(
                executionTable,
                // 조인 결과: row(UserStockRow) + exec(ExecutionRow)
                (row, exec) -> {
                    double currentPrice = (exec == null) ? row.getAvgPrice() : exec.getPrice();
                    double invested = row.getAvgPrice() * row.getHoldingQuantity();
                    double current  = currentPrice        * row.getHoldingQuantity();

                    // 디버깅 로그
                    System.out.println("DEBUG: userId=" + row.getUserId() +
                            ", stockId=" + row.getStockId() +
                            ", avgPrice=" + row.getAvgPrice() +
                            ", currentPrice=" + currentPrice +
                            ", holdingQuantity=" + row.getHoldingQuantity() +
                            ", invested=" + invested +
                            ", current=" + current +
                            ", profitRate=" + ((current - invested) / invested * 100.0));

                    return new PortfolioAgg(invested, current);
                }
        );

        // 4) userId 단위로 합산 (KTable.reduce: add / subtract 둘 다 필요)
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
                        // subtract (해당 키의 이전값이 빠질 때 역연산)
                        (oldV, newV) -> new PortfolioAgg(
                                (oldV==null?0:oldV.getInvested()) - newV.getInvested(),
                                (oldV==null?0:oldV.getCurrent())  - newV.getCurrent()
                        ),
                        Materialized.<String, PortfolioAgg, KeyValueStore<Bytes, byte[]>>as("user-agg-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(aggSerde)
                );

        // 5) 수익률 계산 스트림
        KStream<String, Double> userProfitRate = userAgg
                .toStream()
                .mapValues(PortfolioAgg::profitRatePct);

        // 6) Top10 계산 (StateStore)
        var storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("top10-store"),
                Serdes.String(), Serdes.Double());
        builder.addStateStore(storeBuilder);

        KStream<String, List<RankItem>> top10 = userProfitRate
                .transformValues(() -> new TopNTransformer("top10-store"), "top10-store");

        // 7) JSON 직렬화 후 단일 키로 발행 (예외 처리 포함)
        top10
                .mapValues(list -> {
                    try { return M.writeValueAsString(list); }
                    catch (Exception e) { return "[]"; }
                })
                .selectKey((k,v) -> "TOP10")
                .to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.stream(outTopic, Consumed.with(Serdes.String(), Serdes.String()));
    }

    // ---- Serde helpers (Lombok 클래스 기준: getter 사용) ----
    private Serde<UserStockRow> userStockSerde() {
        var ser = new org.apache.kafka.common.serialization.Serializer<UserStockRow>() {
            @Override public byte[] serialize(String topic, UserStockRow d) {
                if (d == null) return null;
                try {
                    return M.writeValueAsBytes(d);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        var de = new org.apache.kafka.common.serialization.Deserializer<UserStockRow>() {
            @Override public UserStockRow deserialize(String topic, byte[] bytes) {
                if (bytes == null) return null;
                try {
                    return M.readValue(bytes, UserStockRow.class);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
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