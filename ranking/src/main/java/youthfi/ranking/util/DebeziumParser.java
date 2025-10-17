package youthfi.ranking.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import youthfi.ranking.model.ExecutionRow;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public final class DebeziumParser {
    private static final ObjectMapper M = new ObjectMapper();
    private static final DateTimeFormatter DT_MICRO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.n]");

    private DebeziumParser() {}

    public static ExecutionRow parseExecution(String v){
        try {
            JsonNode root = M.readTree(v);
            JsonNode payload = root.path("payload");
            if (payload.isMissingNode()) return null;

            // delete 이벤트 스킵
            if ("d".equals(payload.path("op").asText(""))) return null;

            long tsMs = payload.has("ts_ms") ? payload.path("ts_ms").asLong() : System.currentTimeMillis();

            JsonNode after = payload.path("after");
            if (after.isMissingNode() || after.isNull()) return null;

            // userId / stockId
            String userId  = after.hasNonNull("user_id")  ? after.path("user_id").asText()
                    : after.hasNonNull("userId")  ? String.valueOf(after.path("userId").asLong())
                    : null;
            String stockId = after.hasNonNull("stock_id") ? after.path("stock_id").asText()
                    : after.hasNonNull("stockId") ? after.path("stockId").asText()
                    : null;
            if (userId == null || userId.isBlank() || stockId == null || stockId.isBlank()) return null;

            // is_buy / quantity
            int  isBuy    = after.hasNonNull("is_buy") ? after.path("is_buy").asInt()
                    : after.hasNonNull("isBuy") ? after.path("isBuy").asInt() : 0;
            long quantity = after.path("quantity").isNumber() ? after.path("quantity").longValue()
                    : Long.parseLong(after.path("quantity").asText("0"));
            if (quantity <= 0) return null;

            // price
            BigDecimal priceDec = after.path("price").isNumber()
                    ? after.path("price").decimalValue()
                    : new BigDecimal(after.path("price").asText("0"));
            double price = priceDec.doubleValue();
            if (price <= 0) return null;

            // trade_at: µs(int64) 또는 문자열 → ms
            if (after.has("trade_at")) {
                JsonNode t = after.path("trade_at");
                if (t.isNumber()) {
                    long micros = t.asLong();
                    tsMs = micros / 1000L;
                } else {
                    String s = t.asText();
                    try {
                        LocalDateTime ldt = LocalDateTime.parse(s, DT_MICRO);
                        tsMs = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                    } catch (Exception ignore) {}
                }
            } else if (after.has("tradeAt")) {
                tsMs = after.path("tradeAt").asLong(); // ms 가정
            }

            // 새로 추가: user_balance_snapshot (nullable)
            Double cashAfter = null;
            if (after.has("user_balance_snapshot") && !after.get("user_balance_snapshot").isNull()) {
                JsonNode n = after.get("user_balance_snapshot");
                BigDecimal dec = n.isNumber() ? n.decimalValue() : new BigDecimal(n.asText("0"));
                cashAfter = dec.doubleValue();
            }

            return new ExecutionRow(userId, stockId, price, isBuy, quantity, tsMs, cashAfter);
        } catch (Exception e) {
            return null;
        }
    }
}
