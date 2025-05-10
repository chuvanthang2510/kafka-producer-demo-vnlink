package demo.thangcv.repos;

import demo.thangcv.entitys.FailedKafkaMessage;
import org.springframework.data.jpa.repository.JpaRepository;

public interface FailedKafkaMessageRepository extends JpaRepository<FailedKafkaMessage, Long> {
}
