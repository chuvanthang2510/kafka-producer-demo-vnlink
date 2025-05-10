package demo.thangcv.service;

import org.springframework.stereotype.Service;

@Service
public class MonitoringService {
    public void notifyKafkaFailure(String topic, String message, Throwable ex) {
        // TODO: Gửi cảnh báo qua Email, Slack, Prometheus Alert, v.v.
    }
}
