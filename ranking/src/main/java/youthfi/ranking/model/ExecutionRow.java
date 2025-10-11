package youthfi.ranking.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class ExecutionRow {
    private long userId;
    private String stockId;
    private double price;
}