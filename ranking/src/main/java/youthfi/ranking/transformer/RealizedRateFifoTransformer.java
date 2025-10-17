package youthfi.ranking.transformer;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import youthfi.ranking.model.BuyLot;
import youthfi.ranking.model.ExecutionRow;

import java.util.ArrayDeque;
import java.util.Deque;

public class RealizedRateFifoTransformer implements ValueTransformerWithKey<String, ExecutionRow, Double> {
    private static final ObjectMapper M = new ObjectMapper();

    private final String storeName;
    private KeyValueStore<String, byte[]> store;

    public RealizedRateFifoTransformer(String storeName) {
        this.storeName = storeName;
    }

    @Override @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.store = (KeyValueStore<String, byte[]>) context.getStateStore(storeName);
    }

    @Override
    public Double transform(String key, ExecutionRow e) {
        if (e == null || key == null) return null;

        Deque<BuyLot> q = load(key);

        if (e.getIsBuy() == 1) {
            if (e.getQuantity() > 0 && e.getPrice() > 0) {
                q.addLast(new BuyLot(e.getPrice(), e.getQuantity(), e.getTsMs()));
                save(key, q);
            }
            return null; // 매수만 있을 땐 수익률 없음
        }

        //매도 했으면
        if (e.getIsBuy() == 0) {
            long remain = e.getQuantity();
            if (remain <= 0) return null;

            double pnl = 0.0;
            double cost = 0.0;
            while (remain > 0 && !q.isEmpty()) {
                BuyLot b = q.peekFirst();
                long used = Math.min(remain, b.getQty());
                pnl  += (e.getPrice() - b.getPrice()) * used;
                cost += b.getPrice() * used;

                b.setQty(b.getQty() - used);
                remain -= used;
                if (b.getQty() == 0) q.removeFirst();
            }

            // 초과 매도(remain>0)는 무시
            save(key, q);

            if (cost <= 0.0) return 0.0;
            return (pnl / cost) * 100.0; // 가중평균 실현 수익률
        }

        return null;
    }

    @Override public void close() {}

    // ---- 상태 직렬화/역직렬화(JSON) ----
    private Deque<BuyLot> load(String key) {
        try {
            byte[] bytes = store.get(key);
            if (bytes == null) return new ArrayDeque<>();
            BuyLot[] arr = M.readValue(bytes, BuyLot[].class);
            Deque<BuyLot> q = new ArrayDeque<>();
            for (BuyLot b : arr) q.addLast(b);
            return q;
        } catch (Exception ex) {
            return new ArrayDeque<>();
        }
    }
    private void save(String key, Deque<BuyLot> q) {
        try {
            BuyLot[] arr = q.toArray(new BuyLot[0]);
            store.put(key, M.writeValueAsBytes(arr));
        } catch (Exception ignored) {}
    }
}
