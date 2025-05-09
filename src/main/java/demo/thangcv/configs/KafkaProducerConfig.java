package demo.thangcv.configs;

import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ser.std.StringSerializer;
import demo.thangcv.entitys.TransactionDemo;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaProducerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Bean
    public ProducerFactory<String, TransactionDemo> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, 65536); // 64KB
        configProps.put(ProducerConfig.LINGER_MS_CONFIG, 5); // Đợi 5ms để gom batch
        configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432); // 32MB
        configProps.put(ProducerConfig.ACKS_CONFIG, "1"); // Đảm bảo tất cả broker nhận dữ liệu
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, TransactionDemo> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }
}