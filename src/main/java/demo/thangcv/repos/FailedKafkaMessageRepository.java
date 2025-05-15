package demo.thangcv.repos;

import demo.thangcv.entitys.FailedKafkaMessage;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface FailedKafkaMessageRepository extends JpaRepository<FailedKafkaMessage, Long> {
    List<FailedKafkaMessage> findByRetryCountLessThanAndStatusEquals(Integer retryCount, String status);
    List<FailedKafkaMessage> findByStatus(String status);
}
