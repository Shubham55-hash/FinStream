package com.finstream.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.finstream.exception.TransactionValidationException;
import com.finstream.model.ProcessingResult;
import com.finstream.model.TransactionEvent;
import com.finstream.validation.TransactionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Service responsible for deserializing, validating, and applying monitoring rules to transactions.
 * <p>
 * Note: Duplicate detection in this phase uses an in-memory ConcurrentHashMap set.
 * This is TEMPORARY and limited strictly to the current JVM process lifetime.
 * Durable, database-backed idempotency will be introduced in a future phase.
 */
@Service
public class TransactionProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessingService.class);

    private final TransactionValidator validator;
    private final ObjectMapper objectMapper;
    private final BigDecimal highValueThreshold;

    // Process-lifetime in-memory cache for temporary duplicate detection
    private final Set<String> seenTransactionIds = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final AtomicLong duplicateCount = new AtomicLong(0);

    public TransactionProcessingService(
            TransactionValidator validator,
            ObjectMapper objectMapper,
            @Value("${finstream.monitoring.high-value-threshold:50000.00}") BigDecimal highValueThreshold) {
        this.validator = validator;
        this.objectMapper = objectMapper;
        this.highValueThreshold = highValueThreshold;
    }

    /**
     * Process a raw JSON string received from Kafka.
     *
     * @param rawJson the JSON payload
     * @return the processing result
     */
    public ProcessingResult processRaw(String rawJson) {
        if (rawJson == null || rawJson.trim().isEmpty()) {
            logger.warn("Received empty or null transaction payload");
            return ProcessingResult.malformedJson("Payload is empty or null");
        }

        TransactionEvent event;
        try {
            event = objectMapper.readValue(rawJson, TransactionEvent.class);
        } catch (JsonProcessingException e) {
            logger.error("Failed to parse transaction JSON: {} | Error: {}", rawJson, e.getMessage());
            return ProcessingResult.malformedJson("JSON parsing error: " + e.getMessage());
        }

        return process(event);
    }

    /**
     * Process a deserialized TransactionEvent.
     *
     * @param event the transaction event
     * @return the processing result
     */
    public ProcessingResult process(TransactionEvent event) {
        try {
            validator.validate(event);
        } catch (TransactionValidationException e) {
            logger.warn("[REJECTED] Validation failed for txId '{}': {}", e.getTransactionId(), e.getMessage());
            return ProcessingResult.validationError(e.getTransactionId(), e.getMessage());
        }

        String txId = event.getTransactionId();

        // Temporary process-lifetime duplicate detection
        if (!seenTransactionIds.add(txId)) {
            long totalDupes = duplicateCount.incrementAndGet();
            logger.warn("[DUPLICATE DETECTED] Duplicate transactionId '{}' observed (Process lifetime count: {})",
                    txId, totalDupes);
        }

        // Rule: High-value transaction detection
        if (event.getAmount().compareTo(highValueThreshold) > 0) {
            logger.warn("[MONITORING ALERT] HIGH_VALUE transaction detected! txId='{}', accountId='{}', amount={} {}, merchant='{}'",
                    txId, event.getAccountId(), event.getAmount(), event.getCurrency(), event.getMerchant());
            return ProcessingResult.highValue(txId, event.getAmount().toPlainString());
        }

        logger.info("[ACCEPTED] Transaction processed successfully: txId='{}', accountId='{}', amount={} {}, merchant='{}'",
                txId, event.getAccountId(), event.getAmount(), event.getCurrency(), event.getMerchant());
        return ProcessingResult.accepted(txId);
    }

    public BigDecimal getHighValueThreshold() {
        return highValueThreshold;
    }

    public long getDuplicateCount() {
        return duplicateCount.get();
    }
}
