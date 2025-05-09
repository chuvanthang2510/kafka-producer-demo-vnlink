package demo.thangcv.entitys;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.Instant;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TransactionDemo {
    private String id;
    private Instant timestamp;
    private String userId;
    private BigDecimal amount;
}
