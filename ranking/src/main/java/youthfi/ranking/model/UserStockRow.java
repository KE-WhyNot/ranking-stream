package youthfi.ranking.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class UserStockRow {
    private String userId;
    private String stockId;
    private int holdingQuantity;
    private double avgPrice;
}