package demo.thangcv.service;

import demo.thangcv.entitys.FailedKafkaMessage;
import demo.thangcv.repos.FailedKafkaMessageRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class RetryService {
    private static final int MAX_RETRY_COUNT = 3;
    
    private final FailedKafkaMessageRepository failedMessageRepository;
    private final TransactionProducerSendMessage producer;
    private final MonitoringService monitoringService;

    @Scheduled(fixedRate = 300000) // Chạy mỗi 5 phút
    public void processFailedMessages() {
        log.info("Starting failed messages retry process");
        
        // Lấy các message chưa vượt quá số lần retry
        List<FailedKafkaMessage> failedMessages = 
            failedMessageRepository.findByRetryCountLessThanAndStatusEquals(MAX_RETRY_COUNT, "PENDING");
        
        log.info("Found {} messages to retry", failedMessages.size());
        
        for (FailedKafkaMessage failedMsg : failedMessages) {
            try {
                log.info("Retrying message ID: {}, attempt: {}/{}", 
                    failedMsg.getId(), failedMsg.getRetryCount() + 1, MAX_RETRY_COUNT);
                
                // Thử gửi lại message
                producer.sendTransaction(failedMsg.getPayload());
                
                // Nếu thành công, xóa khỏi database
                failedMessageRepository.delete(failedMsg);
                log.info("Message retry successful: {}", failedMsg.getId());
                
            } catch (Exception e) {
                // Tăng số lần retry
                failedMsg.setRetryCount(failedMsg.getRetryCount() + 1);
                failedMsg.setLastRetryTime(System.currentTimeMillis());
                
                if (failedMsg.getRetryCount() >= MAX_RETRY_COUNT) {
                    // Xử lý khi vượt quá số lần retry
                    handleMaxRetriesExceeded(failedMsg, e);
                } else {
                    // Cập nhật thông tin retry
                    failedMessageRepository.save(failedMsg);
                    log.warn("Message retry failed, attempt {}/{}: {}", 
                        failedMsg.getRetryCount(), MAX_RETRY_COUNT, e.getMessage());
                }
            }
        }
    }

    private void handleMaxRetriesExceeded(FailedKafkaMessage failedMsg, Exception e) {
        log.error("Message exceeded max retries: {}", failedMsg.getId());
        
        // 1. Cập nhật trạng thái
        failedMsg.setStatus("MAX_RETRIES_EXCEEDED");
        failedMessageRepository.save(failedMsg);

        // 2. Thông báo cho team phát triển
        try {
            monitoringService.notifyMaxRetriesExceeded(
                failedMsg.getTopic(),
                failedMsg.getPayload(),
                failedMsg.getRetryCount(),
                e
            );
        } catch (Exception monitorEx) {
            log.error("Failed to send monitoring notification", monitorEx);
        }
    }

    public void retryMessage(FailedKafkaMessage message) {
        if (message.getRetryCount() >= MAX_RETRY_COUNT) {
            throw new IllegalStateException("Message has exceeded maximum retry attempts");
        }
        
        try {
            producer.sendTransaction(message.getPayload());
            failedMessageRepository.delete(message);
            log.info("Manual retry successful for message: {}", message.getId());
        } catch (Exception e) {
            message.setRetryCount(message.getRetryCount() + 1);
            message.setLastRetryTime(System.currentTimeMillis());
            failedMessageRepository.save(message);
            throw new RuntimeException("Retry failed: " + e.getMessage(), e);
        }
    }
} 