package demo.thangcv.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import demo.thangcv.entitys.FailedKafkaMessage;
import demo.thangcv.repos.FailedKafkaMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Service class xử lý việc gửi transaction messages đến Kafka
 * Sử dụng KafkaTemplate để gửi messages và xử lý các callback
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionProducer {
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${app.kafka.topic.transaction}")
    private String topicName;

    @Value("${app.kafka.producer.timeout:5000}")
    private long timeout;

    private final FailedKafkaMessageRepository failedMessageRepository;

    private final MonitoringService monitoringService;

    @Value("${app.kafka.topic.transaction.dlt}")
    private String deadLetterTopic;

    /**
     * Gửi transaction message đến Kafka
     *
     * @param transactionJson JSON string chứa thông tin transaction
     */
    public void sendTransaction(String transactionJson) {
        try {
            // Tạo ProducerRecord với metadata
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topicName,
                    null, // partition sẽ được Kafka tự chọn
                    System.currentTimeMillis(),
                    UUID.randomUUID().toString(), // key ngẫu nhiên
                    transactionJson
            );

            // Thêm headers cho message
            record.headers().add("source", "transaction-producer".getBytes());
            record.headers().add("version", "1.0".getBytes());
            record.headers().add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());

            // Gửi message với retry và callback
            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);

            // Xử lý kết quả gửi message
            future.addCallback(
                    result -> {
                        log.debug("Message sent successfully to topic {} partition {}",
                                topicName, result.getRecordMetadata().partition());
                    },
                    ex -> {
                        log.error("Failed to send message to topic {}: {}", topicName, ex.getMessage());
                        handleFailure(transactionJson, ex);
                    }
            );

            // Đợi kết quả với timeout
            future.get(timeout, TimeUnit.MILLISECONDS);

        } catch (Exception e) {
            log.error("Error sending message to Kafka", e);
            handleFailure(transactionJson, e);
        }
    }

    /**
     * Xử lý khi gửi message thất bại
     *
     * @param message Message gốc
     * @param ex      Exception xảy ra
     */

    private void handleFailure(String message, Throwable ex) {
        log.error("Failed to send message: {}", message, ex);

        // 1. Lưu message vào database
        try {
            FailedKafkaMessage failedMsg = new FailedKafkaMessage();
            failedMsg.setPayload(message);
            failedMsg.setErrorMessage(ex.getMessage());
            failedMsg.setTopic(topicName);
            failedMsg.setCreatedAt(System.currentTimeMillis());
            failedMessageRepository.save(failedMsg);
            log.info("Saved failed message to DB for later retry");
        } catch (Exception dbEx) {
            log.error("Failed to save message to DB", dbEx);
        }

        // 2. Gửi đến Dead Letter Topic
        try {
            ProducerRecord<String, String> dltRecord = new ProducerRecord<>(
                    deadLetterTopic,
                    UUID.randomUUID().toString(),
                    message
            );
            dltRecord.headers().add("original-topic", topicName.getBytes());
            dltRecord.headers().add("error", ex.getMessage().getBytes());
            kafkaTemplate.send(dltRecord);
            log.warn("Message sent to Dead Letter Topic: {}", deadLetterTopic);
        } catch (Exception dltEx) {
            log.error("Failed to send message to Dead Letter Topic", dltEx);
        }

        // 3. Gửi cảnh báo đến hệ thống giám sát
        try {
            monitoringService.notifyKafkaFailure(topicName, message, ex);
        } catch (Exception monitorEx) {
            log.error("Monitoring service failed to notify", monitorEx);
        }
    }
}
