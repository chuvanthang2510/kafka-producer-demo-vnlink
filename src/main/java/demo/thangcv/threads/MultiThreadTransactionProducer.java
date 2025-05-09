package demo.thangcv.threads;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import demo.thangcv.configs.ProducerConfig;
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
@Slf4j
@Component
@RequiredArgsConstructor
public class MultiThreadTransactionProducer implements CommandLineRunner {

    private final TransactionProducer producer;
    private final ThreadConfigRepository threadConfigRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private ThreadPoolExecutor senderExecutor;
    private volatile int currentThreadCount = 0;

    private final BlockingQueue<String> transactionQueue = new LinkedBlockingQueue<>(10_000); // giới hạn tránh tràn RAM

    private static final int SENDER_BATCH_SIZE = 50;
    private static final int SENDER_INTERVAL_MS = 1000;

    @Override
    public void run(String... args) {
        int initialThreadCount = getThreadCountFromDb();
        initThreadPool(initialThreadCount);
        currentThreadCount = initialThreadCount;

        // Khởi tạo các worker sinh giao dịch
        for (int i = 0; i < initialThreadCount; i++) {
            senderExecutor.submit(this::workerTask);
        }

        // Khởi tạo thread gửi batch giao dịch từ queue
        startBatchKafkaSenderWorker();
    }

    @Scheduled(fixedDelay = 60000)
    public void refreshThreadPool() {
        int newThreadCount = getThreadCountFromDb();
        if (newThreadCount != currentThreadCount) {
            senderExecutor.setCorePoolSize(newThreadCount);
            senderExecutor.setMaximumPoolSize(newThreadCount);
            int diff = newThreadCount - currentThreadCount;
            currentThreadCount = newThreadCount;

            if (diff > 0) {
                for (int i = 0; i < diff; i++) {
                    senderExecutor.submit(this::workerTask);
                }
            }
        }
    }

    public int getThreadCountFromDb() {
        List<ThreadConfig> configs = threadConfigRepository.findAll();
        if (!configs.isEmpty()) {
            return configs.get(0).getValue(); // lấy bản ghi đầu tiên
        }
        return 5; // mặc định nếu không có trong DB
    }

    private void initThreadPool(int threadCount) {
        senderExecutor = new ThreadPoolExecutor(
                threadCount,
                threadCount,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }

    private void workerTask() {
        while (true) {
            try {
                List<String> batch = generateBatchTransactions(ProducerConfig.BATCH_SIZE);
                for (String tx : batch) {
                    transactionQueue.put(tx); // chặn nếu queue đầy
                }

                int delay = (60_000 * ProducerConfig.BATCH_SIZE) / ProducerConfig.TX_PER_THREAD_PER_MIN;
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log.error("Error generating transaction", e);
            }
        }
    }

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

    private void startBatchKafkaSenderWorker() {
        Thread senderThread = new Thread(() -> {
            while (true) {
                try {
                    List<String> batch = new ArrayList<>();

                    // Lấy 1 phần tử đầu tiên để tránh block vô thời hạn
                    String first = transactionQueue.take();
                    batch.add(first);

                    // Lấy thêm các phần tử còn lại nếu có (tối đa đến batch size)
                    transactionQueue.drainTo(batch, SENDER_BATCH_SIZE - 1);

                    for (String tx : batch) {
                        producer.sendTransaction(tx); // gửi từng bản ghi
                    }

                    log.info("Sent batch of size: {}", batch.size());

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

