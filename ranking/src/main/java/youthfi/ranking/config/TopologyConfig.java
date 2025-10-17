package youthfi.ranking.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import youthfi.ranking.model.ExecutionRow;
import youthfi.ranking.model.RankItem;
import youthfi.ranking.transformer.RealizedRateFifoTransformer;
import youthfi.ranking.transformer.TopNTransformer;
import youthfi.ranking.util.DebeziumParser;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Configuration
public class TopologyConfig {

    private static final ObjectMapper M = new ObjectMapper();

    @Value("${topics.execution}") String executionTopic;
    @Value("${topics.out}") String outTopic;

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {

        // 0) execution 파싱 (키 = userId|stockId)
        KStream<String, ExecutionRow> execStream = builder
                .stream(executionTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(DebeziumParser::parseExecution)
                .filter((k,v) -> v != null)
                .selectKey((k,v) -> v.getUserId() + "|" + v.getStockId());

        // ★ 3파티션 강제
        KStream<String, ExecutionRow> execP3 = execStream.repartition(
                Repartitioned.<String, ExecutionRow>as("exec-p3")
                        .withNumberOfPartitions(3)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(executionSerde())
        );

        // 1) BUY 롯 상태 스토어
        var lotsStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("buy-lots"),
                Serdes.String(), Serdes.ByteArray());
        builder.addStateStore(lotsStoreBuilder);

        // 2) FIFO 처리 → SELL 시 실현 수익률(Double) 방출
        //    ★ execP3 사용
        KStream<String, Double> realizedRatePerTrade =
                execP3.transformValues(
                        () -> new RealizedRateFifoTransformer("buy-lots"),
                        "buy-lots"
                ).filter((k,v) -> v != null);

        // 3) 유저별 최신 실현 수익률 유지
        KTable<String, Double> userLatestRate = realizedRatePerTrade
                .selectKey((userStockKey, rate) -> userStockKey.split("\\|", 2)[0]) // userId
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((oldV, newV) -> newV,
                        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("user-latest-realized-rate")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                );

        // ★ top10 앞단 3파티션 보장
        KStream<String, Double> ratesP3 = userLatestRate.toStream().repartition(
                Repartitioned.<String, Double>as("rates-p3")
                        .withNumberOfPartitions(3)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Double())
        );

        // 4) Top10 계산 (StateStore)
        var topStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("top10-store"),
                Serdes.String(), Serdes.Double());
        builder.addStateStore(topStoreBuilder);

        // ★ ratesP3 사용
        KStream<String, List<RankItem>> top10 = ratesP3
                .transformValues(() -> new TopNTransformer("top10-store"), "top10-store");

        // 5) JSON 직렬화 후 발행
        top10
                .mapValues(list -> {
                    try { return M.writeValueAsString(list); }
                    catch (Exception e) { return "[]"; }
                })
                .selectKey((k,v) -> "TOP10")
                .to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        return builder.stream(outTopic, Consumed.with(Serdes.String(), Serdes.String()));
    }

    // ---- Serde: ExecutionRow ----
    private Serde<ExecutionRow> executionSerde() {
        var ser = new org.apache.kafka.common.serialization.Serializer<ExecutionRow>() {
            @Override public byte[] serialize(String topic, ExecutionRow d) {
                if (d == null) return null;
                String s = d.getUserId() + "," + d.getStockId() + "," + d.getPrice()
                        + "," + d.getIsBuy() + "," + d.getQuantity() + "," + d.getTsMs();
                return s.getBytes(StandardCharsets.UTF_8);
            }
        };
        var de = new org.apache.kafka.common.serialization.Deserializer<ExecutionRow>() {
            @Override public ExecutionRow deserialize(String topic, byte[] bytes) {
                if (bytes == null) return null;
                String[] p = new String(bytes, StandardCharsets.UTF_8).split(",");
                return new ExecutionRow(
                        p[0],                         // userId
                        p[1],                         // stockId
                        Double.parseDouble(p[2]),     // price
                        Integer.parseInt(p[3]),       // isBuy
                        Long.parseLong(p[4]),         // quantity
                        (p.length >= 6 ? Long.parseLong(p[5]) : System.currentTimeMillis()) // tsMs
                );
            }
        };
        return Serdes.serdeFrom(ser, de);
    }
}
