package com.finstream.consumer;

import com.finstream.model.ProcessingResult;
import com.finstream.service.TransactionProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka listener component consuming financial transaction events.
 */
@Component
public class TransactionKafkaConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TransactionKafkaConsumer.class);

    private final TransactionProcessingService processingService;
    private final AtomicLong consumedCount = new AtomicLong(0);

    public TransactionKafkaConsumer(TransactionProcessingService processingService) {
        this.processingService = processingService;
    }

    @KafkaListener(
            topics = "${finstream.kafka.topic:transactions}",
            groupId = "${spring.kafka.consumer.group-id:finstream-consumer-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload String payload,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        long count = consumedCount.incrementAndGet();
        logger.info("[Kafka Consumer] Received record #{}: partition={}, offset={}, key='{}'",
                count, partition, offset, key);

        try {
            ProcessingResult result = processingService.processRaw(payload);
            logger.debug("Processing completed: txId={}, status={}", result.getTransactionId(), result.getStatus());
        } catch (Exception e) {
            logger.error("Unexpected error while processing transaction payload: {}", payload, e);
        } finally {
            // Acknowledge offset only after processing/logging has executed
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        }
    }

    public long getConsumedCount() {
        return consumedCount.get();
    }

    public void resetConsumedCount() {
        consumedCount.set(0);
    }
}
