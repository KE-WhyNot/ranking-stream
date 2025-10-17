package youthfi.ranking.config;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import youthfi.ranking.transformer.BaselineRateTransformer;
import youthfi.ranking.transformer.TopNTransformer;
import youthfi.ranking.util.DebeziumParser;

import java.util.List;

@Configuration
public class TopologyConfig {

    private static final ObjectMapper M = new ObjectMapper();

    @Value("${topics.execution}") String executionTopic;
    @Value("${topics.out}") String outTopic;

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {

        // 1) execution 파싱
        KStream<String, ExecutionRow> execStream = builder
                .stream(executionTopic, Consumed.with(Serdes.String(), Serdes.String()))
                .mapValues(DebeziumParser::parseExecution)
                .filter((k,v) -> v != null);

        // 2) userId로 rekey (같은 유저 이벤트 순서 보장)
        KStream<String, ExecutionRow> byUser = execStream
                .selectKey((k,v) -> v.getUserId());

        // 3) baseline 상태 스토어 (userId -> baseline cash)
        var baselineStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("baseline-cash-store"),
                Serdes.String(), Serdes.Double()
        );
        builder.addStateStore(baselineStore);

        // 4) 매도 시점의 수익률 계산
        KStream<String, Double> userRate = byUser
                .transformValues(() -> new BaselineRateTransformer("baseline-cash-store"),
                        "baseline-cash-store")
                .filter((userId, rate) -> rate != null);

        // 5) 유저별 최신 수익률 유지
        KTable<String, Double> latestUserRate = userRate
                .groupByKey(Grouped.with(Serdes.String(), Serdes.Double()))
                .reduce((oldV, newV) -> newV,
                        Materialized.<String, Double, KeyValueStore<Bytes, byte[]>>as("user-latest-rate")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.Double())
                );

        // 6) Top10 계산 (StateStore)
        var topStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("top10-store"),
                Serdes.String(), Serdes.Double());
        builder.addStateStore(topStoreBuilder);

        KStream<String, List<RankItem>> top10 = latestUserRate
                .toStream()
                .transformValues(() -> new TopNTransformer("top10-store"), "top10-store");

        // 7) JSON 직렬화 후 발행
        top10
                .mapValues(list -> {
                    try { return M.writeValueAsString(list); }
                    catch (Exception e) { return "[]"; }
                })
                .selectKey((k,v) -> "TOP10")
                .to(outTopic, Produced.with(Serdes.String(), Serdes.String()));

        // optional: 디버깅용 리턴
        return builder.stream(outTopic, Consumed.with(Serdes.String(), Serdes.String()));
    }
}
