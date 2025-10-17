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

            // userId / stockId (snake/camel 모두 지원)
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

            // price: 문자열/숫자 모두 처리
            BigDecimal priceDec = after.path("price").isNumber()
                    ? after.path("price").decimalValue()
                    : new BigDecimal(after.path("price").asText("0"));
            double price = priceDec.doubleValue();
            if (price <= 0) return null;

            // trade_at: µs(int64) 또는 문자열 → ms로 정규화
            if (after.has("trade_at")) {
                JsonNode t = after.path("trade_at");
                if (t.isNumber()) {
                    long micros = t.asLong();     // e.g. 1760732159312309
                    tsMs = micros / 1000L;        // µs → ms
                } else {
                    String s = t.asText();
                    try {
                        // "2025-10-17 11:15:59[.n]" 같은 포맷 지원
                        LocalDateTime ldt = LocalDateTime.parse(s, DT_MICRO);
                        tsMs = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
                    } catch (Exception ignore) {
                        // 실패 시 payload.ts_ms 또는 현재시각 유지
                    }
                }
            } else if (after.has("tradeAt")) {
                tsMs = after.path("tradeAt").asLong(); // ms 가정
            }

            return new ExecutionRow(userId, stockId, price, isBuy, quantity, tsMs);
        } catch (Exception e) {
            return null;
        }
    }

}
