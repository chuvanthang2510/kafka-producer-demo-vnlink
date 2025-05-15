package demo.thangcv.entitys;

import lombok.Data;

import javax.persistence.*;

@Entity
@Data
@Table(name = "PRODUCER_MESSAGE_FAILED")
public class FailedKafkaMessage {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String topic;
    private String payload;
    private String errorMessage;
    private Long createdAt;
    private Integer retryCount = 0;
    private String status = "PENDING";
    private String resolutionNote;
    private Long lastRetryTime;
}
