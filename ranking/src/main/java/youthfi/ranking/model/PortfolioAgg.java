package youthfi.ranking.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter @Setter @NoArgsConstructor @AllArgsConstructor
public class PortfolioAgg {
    private double invested;
    private double current;
    
    public static PortfolioAgg zero() { 
        return new PortfolioAgg(0, 0); 
    }
    
    public PortfolioAgg add(PortfolioAgg o) { 
        return new PortfolioAgg(invested + o.invested, current + o.current); 
    }
    
    public PortfolioAgg sub(PortfolioAgg o) { 
        return new PortfolioAgg(invested - o.invested, current - o.current); 
    }
    
    public double profitRatePct() {
        return invested <= 0 ? 0.0 : (current - invested) / invested * 100.0;
    }
}
