package youthfi.ranking.transformer;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import youthfi.ranking.model.RankItem;

import java.util.ArrayList;
import java.util.List;

public class TopNTransformer implements ValueTransformerWithKey<String, Double, List<RankItem>> {
    private final String storeName;
    private KeyValueStore<String, Double> store;

    public TopNTransformer(String storeName){ this.storeName = storeName; }

    @Override @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.store = (KeyValueStore<String, Double>) context.getStateStore(storeName);
    }

    @Override
    public List<RankItem> transform(String userId, Double profitRate) {
        if (userId == null) return List.of();

        if (profitRate == null) {
            store.delete(userId);           // tombstone: 삭제
        } else {
            store.put(userId, profitRate);  // 0%도 정상 저장
        }

        List<KeyValue<String, Double>> all = new ArrayList<>();
        try (KeyValueIterator<String, Double> it = store.all()) {
            while (it.hasNext()) all.add(it.next());
        }

        all.sort((a, b) -> Double.compare(b.value, a.value)); // 내림차순

        List<RankItem> top10 = new ArrayList<>();
        for (int i = 0; i < Math.min(10, all.size()); i++) {
            var e = all.get(i);
            var item = new RankItem();
            item.setUserId(e.key);
            item.setRankNo(i + 1);
            item.setProfitRate(e.value);
            top10.add(item);
        }
        return top10;
    }

    @Override public void close() {}
}