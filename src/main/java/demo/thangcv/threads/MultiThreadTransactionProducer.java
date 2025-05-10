package demo.thangcv.threads;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import demo.thangcv.entitys.ThreadConfig;
import demo.thangcv.repos.ThreadConfigRepository;
import demo.thangcv.service.TransactionProducer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Class xử lý việc gửi transaction messages theo đa luồng
 * Sử dụng thread pool để quản lý các worker threads
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class MultiThreadTransactionProducer implements CommandLineRunner {

    private final TransactionProducer producer;
    private final ThreadConfigRepository threadConfigRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    // Thread pool để quản lý các worker threads
    private ThreadPoolExecutor senderExecutor;
    // Số lượng thread hiện tại
    private volatile int currentThreadCount = 0;

    // Queue để lưu trữ các transaction chờ gửi
    private final BlockingQueue<String> transactionQueue = new LinkedBlockingQueue<>(10_000);

    // Các hằng số cấu hình
    private static final int SENDER_BATCH_SIZE = 50;
    private static final int SENDER_INTERVAL_MS = 1000;
    private static final int TX_PER_THREAD_PER_MIN = 300;

    /**
     * Khởi tạo và chạy producer khi ứng dụng khởi động
     */
    @Override
    public void run(String... args) {
        // Lấy số lượng thread từ database
        int initialThreadCount = getThreadCountFromDb();
        // Khởi tạo thread pool
        initThreadPool(initialThreadCount);
        currentThreadCount = initialThreadCount;

        // Khởi tạo các worker threads
        for (int i = 0; i < initialThreadCount; i++) {
            senderExecutor.submit(this::workerTask);
        }

        // Khởi tạo thread gửi batch messages
        startBatchKafkaSenderWorker();
    }

    /**
     * Cập nhật số lượng thread mỗi phút
     */
    @Scheduled(fixedDelay = 60000)
    public void refreshThreadPool() {
        int newThreadCount = getThreadCountFromDb();
        if (newThreadCount != currentThreadCount) {
            // Cập nhật kích thước thread pool
            senderExecutor.setCorePoolSize(newThreadCount);
            senderExecutor.setMaximumPoolSize(newThreadCount);
            int diff = newThreadCount - currentThreadCount;
            currentThreadCount = newThreadCount;

            // Thêm threads mới nếu cần
            if (diff > 0) {
                for (int i = 0; i < diff; i++) {
                    senderExecutor.submit(this::workerTask);
                }
            }
        }
    }

    /**
     * Lấy số lượng thread từ database
     */
    private int getThreadCountFromDb() {
        List<ThreadConfig> configs = threadConfigRepository.findAll();
        if (!configs.isEmpty()) {
            return configs.get(0).getValue();
        }
        return 5; // Giá trị mặc định
    }

    /**
     * Khởi tạo thread pool với số lượng thread được chỉ định
     */
    private void initThreadPool(int threadCount) {
        senderExecutor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(1);
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread thread = new Thread(r);
                        thread.setName("kafka-producer-" + counter.getAndIncrement());
                        thread.setUncaughtExceptionHandler((t, e) -> 
                            log.error("Uncaught exception in thread " + t.getName(), e));
                        return thread;
                    }
                },
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    /**
     * Task của mỗi worker thread
     * Sinh và đưa transactions vào queue
     */
    private void workerTask() {
        while (true) {
            try {
                // Sinh batch transactions
                List<String> batch = generateBatchTransactions(10);
                for (String tx : batch) {
                    transactionQueue.put(tx);
                }

                // Tính toán delay để đảm bảo rate limit
                int delay = (60_000 * 10) / TX_PER_THREAD_PER_MIN;
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error generating transaction", e);
            }
        }
    }

    /**
     * Sinh một batch transactions
     */
    private List<String> generateBatchTransactions(int count) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            Map<String, Object> tx = new HashMap<>();
            tx.put("transactionId", UUID.randomUUID().toString());
            tx.put("amount", ThreadLocalRandom.current().nextDouble(10, 1000));
            tx.put("timestamp", Instant.now().toString());
            try {
                list.add(objectMapper.writeValueAsString(tx));
            } catch (JsonProcessingException e) {
                log.error("Error serializing transaction", e);
            }
        }
        return list;
    }

    /**
     * Khởi tạo thread gửi batch messages đến Kafka
     */
    private void startBatchKafkaSenderWorker() {
        Thread senderThread = new Thread(() -> {
            while (true) {
                try {
                    List<String> batch = new ArrayList<>();

                    // Lấy message đầu tiên từ queue
                    String first = transactionQueue.take();
                    batch.add(first);

                    // Lấy thêm các messages còn lại
                    transactionQueue.drainTo(batch, SENDER_BATCH_SIZE - 1);

                    // Gửi batch messages
                    for (String tx : batch) {
                        producer.sendTransaction(tx);
                    }

                    log.info("Sent batch of size: {}", batch.size());

                    // Đợi một khoảng thời gian trước khi gửi batch tiếp theo
                    Thread.sleep(SENDER_INTERVAL_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.error("Error sending batch to Kafka", e);
                }
            }
        });

        senderThread.setDaemon(true);
        senderThread.setName("kafka-batch-sender");
        senderThread.start();
    }
}

