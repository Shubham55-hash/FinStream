package com.finstream.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.finstream.exception.TransactionValidationException;
import com.finstream.model.ProcessingResult;
import com.finstream.model.TransactionEntity;
import com.finstream.model.TransactionEvent;
import com.finstream.repository.TransactionRepository;
import com.finstream.validation.TransactionValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;

/**
 * Core processing service orchestrating validation, monitoring rules,
 * persistent idempotency, and alert generation.
 */
@Service
public class TransactionProcessingService {

    private static final Logger logger = LoggerFactory.getLogger(TransactionProcessingService.class);

    private final TransactionValidator validator;
    private final TransactionRepository repository;
    private final ObjectMapper objectMapper;
    private final BigDecimal highValueThreshold;
    private final int rapidSpendingCount;
    private final int rapidSpendingWindowSeconds;

    public TransactionProcessingService(
            TransactionValidator validator,
            TransactionRepository repository,
            ObjectMapper objectMapper,
            @Value("${finstream.monitoring.high-value-threshold:50000.00}") BigDecimal highValueThreshold,
            @Value("${finstream.monitoring.rapid-spending-count:3}") int rapidSpendingCount,
            @Value("${finstream.monitoring.rapid-spending-window-seconds:60}") int rapidSpendingWindowSeconds) {
        this.validator = validator;
        this.repository = repository;
        this.objectMapper = objectMapper;
        this.highValueThreshold = highValueThreshold;
        this.rapidSpendingCount = rapidSpendingCount;
        this.rapidSpendingWindowSeconds = rapidSpendingWindowSeconds;
    }

    /**
     * Process raw JSON payload received from Kafka.
     */
    public ProcessingResult processRaw(String rawJson) {
        if (rawJson == null || rawJson.trim().isEmpty()) {
            logger.warn("Received empty or null transaction payload");
            try {
                repository.saveRejectedEvent(null, "EMPTY_PAYLOAD", "Payload is empty or null", "MALFORMED_JSON");
            } catch (Exception e) {
                logger.error("Failed to persist rejected event to DB", e);
            }
            return ProcessingResult.malformedJson("Payload is empty or null");
        }

        TransactionEvent event;
        try {
            event = objectMapper.readValue(rawJson, TransactionEvent.class);
        } catch (JsonProcessingException e) {
            logger.error("Failed to parse transaction JSON: {} | Error: {}", rawJson, e.getMessage());
            try {
                repository.saveRejectedEvent(null, rawJson, "JSON parsing error: " + e.getMessage(), "MALFORMED_JSON");
            } catch (Exception dbEx) {
                logger.error("Failed to persist rejected event to DB", dbEx);
            }
            return ProcessingResult.malformedJson("JSON parsing error: " + e.getMessage());
        }

        return process(event, rawJson);
    }

    /**
     * Validate, monitor, and persist the transaction event.
     */
    @Transactional
    public ProcessingResult process(TransactionEvent event, String rawJson) {
        String txId = event != null ? event.getTransactionId() : "UNKNOWN";

        // 1. Validation
        try {
            validator.validate(event);
        } catch (TransactionValidationException e) {
            logger.warn("[REJECTED] Validation failed for txId '{}': {}", e.getTransactionId(), e.getMessage());
            try {
                repository.saveRejectedEvent(e.getTransactionId(), rawJson != null ? rawJson : event.toString(),
                        e.getMessage(), "VALIDATION_ERROR");
            } catch (Exception dbEx) {
                logger.error("Failed to record validation failure in DB", dbEx);
            }
            return ProcessingResult.validationError(e.getTransactionId(), e.getMessage());
        }

        // 2. Persistent Idempotency Check
        Optional<TransactionEntity> existingOpt = repository.findByTransactionId(txId);
        if (existingOpt.isPresent()) {
            TransactionEntity existing = existingOpt.get();
            if (existing.matchesEvent(event)) {
                logger.info("[DUPLICATE DETECTED] Duplicate transactionId '{}' observed with identical data. Skipping insert.", txId);
                return ProcessingResult.duplicate(txId);
            } else {
                logger.warn("[SECURITY ALERT] ID COLLISION detected for txId '{}'! Incoming data differs from stored transaction.", txId);
                repository.saveRejectedEvent(txId, rawJson != null ? rawJson : event.toString(),
                        "Transaction ID collision with conflicting data", "ID_COLLISION");
                repository.saveAlert(txId, event.getAccountId(), "ID_COLLISION",
                        "Collision: Duplicate txId with conflicting transaction data");
                return ProcessingResult.collision(txId, "Conflicting data for existing transactionId");
            }
        }

        // 3. Rule Evaluation: High-Value (> 50,000 INR strictly)
        boolean isHighValue = event.getAmount().compareTo(highValueThreshold) > 0;

        // 4. Rule Evaluation: Rapid-Spending (>= 3 distinct transactions in rolling 60s)
        OffsetDateTime windowEnd = event.getTimestamp();
        OffsetDateTime windowStart = windowEnd.minusSeconds(rapidSpendingWindowSeconds);
        int existingInWindow = repository.countTransactionsInWindow(event.getAccountId(), windowStart, windowEnd);
        int totalInWindow = existingInWindow + 1; // including current event
        boolean isRapidSpending = totalInWindow >= rapidSpendingCount;

        // Determine consolidated status
        String status;
        if (isHighValue && isRapidSpending) {
            status = "HIGH_VALUE_AND_RAPID_SPENDING";
        } else if (isHighValue) {
            status = "HIGH_VALUE";
        } else if (isRapidSpending) {
            status = "RAPID_SPENDING";
        } else {
            status = "ACCEPTED";
        }

        // 5. Persist Transaction
        TransactionEntity entity = new TransactionEntity(
                event.getTransactionId(),
                event.getAccountId(),
                event.getAmount(),
                event.getCurrency(),
                event.getTimestamp(),
                event.getMerchant(),
                status
        );

        try {
            repository.saveTransaction(entity);
        } catch (DuplicateKeyException e) {
            // Concurrent race safety: another consumer thread just inserted the same ID
            logger.warn("Caught DuplicateKeyException for txId '{}' during concurrent insertion. Re-evaluating.", txId);
            return handleConcurrentDuplicate(event, rawJson);
        }

        // 6. Persist Alerts
        if (isHighValue) {
            repository.saveAlert(
                    txId,
                    event.getAccountId(),
                    "HIGH_VALUE",
                    String.format("Amount %s INR exceeds high-value threshold (%s INR)",
                            event.getAmount().toPlainString(), highValueThreshold.toPlainString())
            );
        }

        if (isRapidSpending) {
            repository.saveAlert(
                    txId,
                    event.getAccountId(),
                    "RAPID_SPENDING",
                    String.format("Account %s reached %d transactions within rolling %d seconds",
                            event.getAccountId(), totalInWindow, rapidSpendingWindowSeconds)
            );
        }

        // 7. Return Result
        if (isHighValue && isRapidSpending) {
            logger.warn("[MONITORING ALERT] BOTH RULES TRIGGERED! txId='{}', accountId='{}', amount={} {}, windowCount={}",
                    txId, event.getAccountId(), event.getAmount(), event.getCurrency(), totalInWindow);
            return ProcessingResult.highValueAndRapidSpending(txId, event.getAmount().toPlainString(), totalInWindow, rapidSpendingWindowSeconds);
        } else if (isHighValue) {
            logger.warn("[MONITORING ALERT] HIGH_VALUE detected! txId='{}', accountId='{}', amount={} {}",
                    txId, event.getAccountId(), event.getAmount(), event.getCurrency());
            return ProcessingResult.highValue(txId, event.getAmount().toPlainString());
        } else if (isRapidSpending) {
            logger.warn("[MONITORING ALERT] RAPID_SPENDING detected! txId='{}', accountId='{}', count={}/{}s",
                    txId, event.getAccountId(), totalInWindow, rapidSpendingWindowSeconds);
            return ProcessingResult.rapidSpending(txId, totalInWindow, rapidSpendingWindowSeconds);
        }

        logger.info("[ACCEPTED] Transaction processed and persisted: txId='{}', accountId='{}', amount={} {}",
                txId, event.getAccountId(), event.getAmount(), event.getCurrency());
        return ProcessingResult.accepted(txId);
    }

    private ProcessingResult handleConcurrentDuplicate(TransactionEvent event, String rawJson) {
        Optional<TransactionEntity> existing = repository.findByTransactionId(event.getTransactionId());
        if (existing.isPresent() && existing.get().matchesEvent(event)) {
            return ProcessingResult.duplicate(event.getTransactionId());
        }
        return ProcessingResult.collision(event.getTransactionId(), "Concurrent ID collision");
    }

    public BigDecimal getHighValueThreshold() {
        return highValueThreshold;
    }

    public int getRapidSpendingCount() {
        return rapidSpendingCount;
    }

    public int getRapidSpendingWindowSeconds() {
        return rapidSpendingWindowSeconds;
    }
}
