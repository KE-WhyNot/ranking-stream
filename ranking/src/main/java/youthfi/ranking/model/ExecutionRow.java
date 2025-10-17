package youthfi.ranking.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class ExecutionRow {
    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("stock_id")
    private String stockId;

    @JsonProperty("price")
    private double price;

    @JsonProperty("is_buy")
    private int isBuy;

    @JsonProperty("quantity")
    private long quantity;

    @JsonProperty("ts_ms")
    private long tsMs;

    @JsonProperty("user_balance_snapshot")
    private Double userBalanceSnapshot;

}
