package youthfi.ranking.model;

import lombok.*;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class BuyLot {
    private double price;  // 매수가
    private long   qty;    // 남은 수량
    private long   tsMs;   // 매수 시각
}
