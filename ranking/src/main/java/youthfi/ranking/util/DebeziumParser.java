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

            String op = payload.path("op").asText("");
            if ("d".equals(op)) return null; // 삭제 이벤트 무시

            long fallbackTs = payload.has("ts_ms") ? payload.path("ts_ms").asLong() : System.currentTimeMillis();
            JsonNode after = payload.path("after");
            if (after.isMissingNode() || after.isNull()) return null;

            String userId  = after.path("user_id").asText(null);
            String stockId = after.path("stock_id").asText(null);
            if (userId == null || userId.isBlank() || stockId == null || stockId.isBlank()) return null;

            // price decimal 안전 파싱
            BigDecimal priceDec = after.path("price").isNumber()
                    ? after.path("price").decimalValue()
                    : new BigDecimal(after.path("price").asText("0"));
            double price = priceDec.doubleValue();
            if (price <= 0) return null;

            int isBuy = after.path("is_buy").asInt();

            long qty = after.path("quantity").isNumber()
                    ? after.path("quantity").longValue()
                    : Long.parseLong(after.path("quantity").asText("0"));
            if (qty <= 0) return null;

            long tsMs = fallbackTs;
            String tradeAtStr = after.path("trade_at").asText(null);
            if (tradeAtStr != null && !tradeAtStr.isBlank()) {
                LocalDateTime ldt = LocalDateTime.parse(tradeAtStr, DT_MICRO);
                // 필요 시 Asia/Seoul로 바꿔도 됨. 여기선 UTC 기준.
                tsMs = ldt.toInstant(ZoneOffset.UTC).toEpochMilli();
            }

            return new ExecutionRow(userId, stockId, price, isBuy, qty, tsMs);
        } catch (Exception e) {
            return null;
        }
    }
}
