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
public class UserStockRow {
    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("stock_id")
    private String stockId;

    @JsonProperty("holding_quantity")
    private int holdingQuantity;

    @JsonProperty("avg_price")
    private double avgPrice;
}