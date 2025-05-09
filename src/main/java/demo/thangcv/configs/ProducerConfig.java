package demo.thangcv.configs;

import org.springframework.context.annotation.Configuration;

@Configuration
public class ProducerConfig {
    public static final int TX_PER_THREAD_PER_MIN = 300; // mỗi luồng gửi 300 tx/phút
    public static final int BATCH_SIZE = 10; // số tx gửi/lần
}