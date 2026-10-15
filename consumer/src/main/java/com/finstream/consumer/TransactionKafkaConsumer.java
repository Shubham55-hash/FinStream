package com.finstream.consumer;

import com.finstream.model.ProcessingResult;
import com.finstream.service.TransactionProcessingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Kafka consumer listener with explicit manual offset acknowledgment
 * and deliberate database failure propagation.
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
            logger.info("[Kafka Consumer] Processed txId='{}': status={}", result.getTransactionId(), result.getStatus());

            // Acknowledge offset ONLY after validation, monitoring, and database persistence succeed
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
            }
        } catch (DataAccessException e) {
            // Database failure: DO NOT acknowledge offset!
            // Throw exception so Spring Kafka triggers bounded retries without losing the message
            logger.error("[Kafka Consumer] Database failure for offset {} (key='{}'). NOT acknowledging offset: {}",
                    offset, key, e.getMessage());
            throw e;
        } catch (Exception e) {
            logger.error("[Kafka Consumer] Unexpected fatal failure for offset {} (key='{}'): {}",
                    offset, key, e.getMessage(), e);
            throw e;
        }
    }

    public long getConsumedCount() {
        return consumedCount.get();
    }

    public void resetConsumedCount() {
        consumedCount.set(0);
    }
}
