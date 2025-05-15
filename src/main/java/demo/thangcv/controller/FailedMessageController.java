package demo.thangcv.controller;

import demo.thangcv.entitys.FailedKafkaMessage;
import demo.thangcv.repos.FailedKafkaMessageRepository;
import demo.thangcv.service.RetryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/failed-messages")
@RequiredArgsConstructor
public class FailedMessageController {
    
    private final FailedKafkaMessageRepository repository;
    private final RetryService retryService;

    @GetMapping
    public List<FailedKafkaMessage> getAllFailedMessages() {
        return repository.findAll();
    }

    @GetMapping("/max-retries")
    public List<FailedKafkaMessage> getMaxRetriesMessages() {
        return repository.findByStatus("MAX_RETRIES_EXCEEDED");
    }

    @PostMapping("/{id}/retry")
    public ResponseEntity<?> manualRetry(@PathVariable Long id) {
        FailedKafkaMessage message = repository.findById(id)
            .orElseThrow(() -> new RuntimeException("Message not found"));
            
        try {
            retryService.retryMessage(message);
            return ResponseEntity.ok().build();
        } catch (Exception e) {
            return ResponseEntity.status(500)
                .body("Retry failed: " + e.getMessage());
        }
    }

    @PostMapping("/{id}/resolve")
    public ResponseEntity<?> markAsResolved(
        @PathVariable Long id,
        @RequestBody String resolutionNote
    ) {
        FailedKafkaMessage message = repository.findById(id)
            .orElseThrow(() -> new RuntimeException("Message not found"));
            
        message.setStatus("RESOLVED");
        message.setResolutionNote(resolutionNote);
        repository.save(message);
        
        return ResponseEntity.ok().build();
    }
} 