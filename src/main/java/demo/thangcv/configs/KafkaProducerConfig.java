package demo.thangcv.configs;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Cấu hình cho Kafka Producer
 * Chứa các cấu hình quan trọng cho việc gửi messages đến Kafka
 */
@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    /**
     * Tạo ProducerFactory với các cấu hình tối ưu
     * @return ProducerFactory đã được cấu hình
     */
    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        
        // Cấu hình cơ bản
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        
        // Tối ưu performance
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64KB batch size
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 5); // Đợi 5ms để gom batch
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB buffer
        configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // Nén dữ liệu
        
        // Tối ưu reliability
        configProps.put(ProducerConfig.ACKS_CONFIG, "all"); // Đảm bảo tất cả replicas nhận được
        configProps.put(ProducerConfig.RETRIES_CONFIG, 3); // Số lần retry
        configProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000); // Thời gian chờ giữa các lần retry
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true); // Đảm bảo không gửi trùng message
        
        // Tối ưu monitoring
        configProps.put(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG, 30000); // Cửa sổ lấy mẫu metrics
        configProps.put(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG, 2); // Số mẫu metrics
        
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    /**
     * Tạo KafkaTemplate với ProducerFactory đã cấu hình
     * @return KafkaTemplate đã được cấu hình
     */
    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}