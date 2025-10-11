package youthfi.ranking.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
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
                .toTable(Materialized.as("userstock-table"));

        // 2) execution: GlobalKTable (key = stockId, value = raw json string)
        GlobalKTable<String, String> executionGT = builder.globalTable(
                executionTopic,
                Consumed.with(Serdes.String(), Serdes.String()),
                Materialized.with(Serdes.String(), Serdes.String())
        );

        // 3) 외래키 조인: userstock(row)에서 stockId 추출 → GlobalKTable lookup
        KStream<String, PortfolioAgg> userPositions = userStock
                .toStream() // key = userId|stockId
                .leftJoin(
                        executionGT,
                        // 외래키: userId|stockId → stockId
                        (userIdStockId, row) -> row.getStockId(),
                        // 조인 결과: row(UserStockRow) + execJson(String)
                        (row, execJson) -> {
                            ExecutionRow exec = DebeziumParser.parseExecution(execJson);
                            double currentPrice = (exec == null) ? row.getAvgPrice() : exec.getPrice();
                            double invested = row.getAvgPrice() * row.getHoldingQuantity();
                            double current  = currentPrice        * row.getHoldingQuantity();
                            return new PortfolioAgg(invested, current);
                        }
                )
                // key = userId
                .selectKey((userIdStockId, agg) -> userIdStockId.split("\\|", 2)[0]);

        // 4) 사용자별 합산 (간단 reduce) — 소규모 트래픽 전제
        Serde<PortfolioAgg> aggSerde = portfolioSerde();
        KTable<String, PortfolioAgg> userAgg = userPositions
                .groupByKey(Grouped.with(Serdes.String(), aggSerde))
                .reduce(
                        (oldV, newV) -> new PortfolioAgg(
                                (oldV==null?0:oldV.getInvested()) + newV.getInvested(),
                                (oldV==null?0:oldV.getCurrent())  + newV.getCurrent()
                        ),
                        Materialized.with(Serdes.String(), aggSerde)
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
}