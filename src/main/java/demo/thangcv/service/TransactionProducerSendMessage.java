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
 * 
 * Các chức năng chính:
 * 1. Gửi message đến Kafka topic
 * 2. Xử lý lỗi và retry
 * 3. Lưu message thất bại vào database
 * 4. Gửi message thất bại đến Dead Letter Topic
 * 5. Thông báo cho hệ thống monitoring
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class TransactionProducerSendMessage {
    // KafkaTemplate để gửi messages đến Kafka
    private final KafkaTemplate<String, String> kafkaTemplate;
    
    // ObjectMapper để xử lý JSON
    private final ObjectMapper objectMapper;
    
    // Repository để lưu các message thất bại
    private final FailedKafkaMessageRepository failedMessageRepository;
    
    // Service để gửi thông báo đến hệ thống monitoring abc
    private final MonitoringService monitoringService;

    // Topic chính để gửi messages
    @Value("${app.kafka.topic.transaction}")
    private String topicName;

    // Thời gian timeout khi gửi message (mặc định 5 giây)
    @Value("${app.kafka.producer.timeout:5000}")
    private long timeout;

    // Topic để gửi các message thất bại (Dead Letter Topic)
    @Value("${app.kafka.topic.transaction.dlt}")
    private String deadLetterTopic;

    /**
     * Gửi transaction message đến Kafka
     * 
     * Quy trình:
     * 1. Tạo ProducerRecord với metadata
     * 2. Thêm headers cho message
     * 3. Gửi message và xử lý callback
     * 4. Đợi kết quả với timeout
     * 5. Xử lý lỗi nếu có
     *
     * @param transactionJson JSON string chứa thông tin transaction
     */
    public void sendTransaction(String transactionJson) {
        try {
            // Tạo ProducerRecord với metadata
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    topicName,                    // Topic đích
                    null,                         // Partition (null = Kafka tự chọn)
                    System.currentTimeMillis(),   // Timestamp
                    UUID.randomUUID().toString(), // Key ngẫu nhiên
                    transactionJson               // Message payload
            );

            // Thêm headers cho message để tracking và debugging
            record.headers().add("source", "transaction-producer".getBytes());
            record.headers().add("version", "1.0".getBytes());
            record.headers().add("timestamp", String.valueOf(System.currentTimeMillis()).getBytes());

            // Gửi message và xử lý callback
            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(record);
            
            // Xử lý kết quả gửi message
            future.addCallback(
                    // Callback khi gửi thành công
                    result -> {
                        log.debug("Message sent successfully to topic {} partition {}",
                                topicName, result.getRecordMetadata().partition());
                    },
                    // Callback khi gửi thất bại
                    ex -> {
                        log.error("Failed to send message to topic {}: {}", topicName, ex.getMessage());
                        handleFailure(transactionJson, ex);
                    }
            );

            // Đợi kết quả với timeout
            future.get(timeout, TimeUnit.MILLISECONDS);
            
        } catch (Exception e) {
            // Xử lý tất cả các lỗi có thể xảy ra
            log.error("Error sending message to Kafka", e);
            handleFailure(transactionJson, e);
        }
    }

    /**
     * Xử lý khi gửi message thất bại
     * 
     * Quy trình xử lý lỗi:
     * 1. Lưu message vào database để retry sau
     * 2. Gửi message đến Dead Letter Topic
     * 3. Thông báo cho hệ thống monitoring
     *
     * @param message Message gốc bị lỗi
     * @param ex Exception xảy ra
     */
    private void handleFailure(String message, Throwable ex) {
        log.error("Failed to send message: {}", message, ex);

        // 1. Lưu message vào database để retry sau
        try {
            FailedKafkaMessage failedMsg = new FailedKafkaMessage();
            failedMsg.setPayload(message);
            failedMsg.setErrorMessage(ex.getMessage());
            failedMsg.setTopic(topicName);
            failedMsg.setCreatedAt(System.currentTimeMillis());
            failedMsg.setStatus("PENDING");
            failedMsg.setRetryCount(0);
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
            // Thêm metadata cho message trong DLT
            dltRecord.headers().add("original-topic", topicName.getBytes());
            dltRecord.headers().add("error", ex.getMessage().getBytes());
            kafkaTemplate.send(dltRecord);
            log.warn("Message sent to Dead Letter Topic: {}", deadLetterTopic);
        } catch (Exception dltEx) {
            log.error("Failed to send message to Dead Letter Topic", dltEx);
        }

        // 3. Gửi cảnh báo đến hệ thống giám sát
        try {
            monitoringService.notifyKafkaFailure(topicName, message, (Exception) ex);
        } catch (Exception monitorEx) {
            log.error("Monitoring service failed to notify", monitorEx);
        }
    }
}
