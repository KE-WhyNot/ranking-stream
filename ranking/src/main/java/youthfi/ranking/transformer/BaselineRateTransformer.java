package youthfi.ranking.transformer;

import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import youthfi.ranking.model.ExecutionRow;

public class BaselineRateTransformer implements ValueTransformerWithKey<String, ExecutionRow, Double> {
    private final String storeName;
    private KeyValueStore<String, Double> baselineStore;

    private static final double FIRST_BASELINE = 10_000_000.0; // 최초 1천만원

    public BaselineRateTransformer(String storeName) { this.storeName = storeName; }

    @Override @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.baselineStore = (KeyValueStore<String, Double>) context.getStateStore(storeName);
    }

    @Override
    public Double transform(String userId, ExecutionRow e) {
        if (e == null || userId == null) return null;

        // 매도 + 잔고 스냅샷 있는 경우만
        if (e.getIsBuy() != 0) return null;
        if (e.getUserBalanceSnapshot() == null) return null;

        double cashAfter = e.getUserBalanceSnapshot();
        Double base = baselineStore.get(userId);
        if (base == null || base <= 0.0) base = FIRST_BASELINE;

        double rate = (cashAfter - base) / base * 100.0;

        // 다음 매도 대비 baseline 갱신
        baselineStore.put(userId, cashAfter);

        // NaN / Infinity 방지
        if (Double.isNaN(rate) || Double.isInfinite(rate)) return null;
        return rate;
    }

    @Override public void close() {}
}
