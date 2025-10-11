package youthfi.ranking.util;

import youthfi.ranking.model.ExecutionRow;
import youthfi.ranking.model.UserStockRow;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
public final class DebeziumParser {
    private static final ObjectMapper M = new ObjectMapper();

    public static UserStockRow parseUserStock(String v){
        try {
            JsonNode after = M.readTree(v).path("payload").path("after");
            if (after.isMissingNode() || after.isNull()) return null;
            long userId = after.path("userId").asLong();
            String stockId = after.path("stockId").asText();
            int qty = after.path("holdingQuantity").asInt();
            double avg = after.path("avgPrice").asDouble();
            return new UserStockRow(userId, stockId, qty, avg);
        } catch (Exception e) { return null; }
    }

    public static ExecutionRow parseExecution(String v){
        try {
            JsonNode after = M.readTree(v).path("payload").path("after");
            if (after.isMissingNode() || after.isNull()) return null;
            String stockId = after.path("stockId").asText();
            double price = after.path("price").asDouble();
            return new ExecutionRow(stockId, price);
        } catch (Exception e) { return null; }
    }
}
