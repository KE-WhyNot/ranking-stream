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
        if (userId == null || profitRate == null) return List.of();
        store.put(userId, profitRate);

        List<KeyValue<String, Double>> all = new ArrayList<>();
        try (KeyValueIterator<String, Double> it = store.all()) {
            while (it.hasNext()) {
                all.add(it.next()); // it.next() 는 KeyValue<String, Double>
            }
        }

        all.sort((a, b) -> Double.compare(b.value, a.value)); // 내림차순

        List<RankItem> top10 = new ArrayList<>();
        for (int i = 0; i < Math.min(10, all.size()); i++) {
            KeyValue<String, Double> e = all.get(i);
            RankItem item = new RankItem();
            item.setUserId(Long.parseLong(e.key));
            item.setRank(i + 1);
            item.setProfitRate(e.value);
            top10.add(item);
        }
        return top10;
    }

    @Override public void close() {}
}
