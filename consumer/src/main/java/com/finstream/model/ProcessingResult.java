package com.finstream.model;

import java.time.Instant;

/**
 * Encapsulates the outcome of validating, monitoring, and persisting a transaction.
 */
public class ProcessingResult {

    public enum Status {
        ACCEPTED,
        FLAGGED_HIGH_VALUE,
        FLAGGED_RAPID_SPENDING,
        FLAGGED_HIGH_VALUE_AND_RAPID_SPENDING,
        DUPLICATE_DELIVERY,
        ID_COLLISION,
        REJECTED_VALIDATION_ERROR,
        REJECTED_MALFORMED_JSON
    }

    private final String transactionId;
    private final Status status;
    private final String reason;
    private final boolean highValueFlag;
    private final boolean rapidSpendingFlag;
    private final Instant processedAt;

    private ProcessingResult(String transactionId, Status status, String reason,
                             boolean highValueFlag, boolean rapidSpendingFlag) {
        this.transactionId = transactionId;
        this.status = status;
        this.reason = reason;
        this.highValueFlag = highValueFlag;
        this.rapidSpendingFlag = rapidSpendingFlag;
        this.processedAt = Instant.now();
    }

    public static ProcessingResult accepted(String transactionId) {
        return new ProcessingResult(transactionId, Status.ACCEPTED, "Transaction accepted and persisted", false, false);
    }

    public static ProcessingResult highValue(String transactionId, String amountStr) {
        return new ProcessingResult(
                transactionId,
                Status.FLAGGED_HIGH_VALUE,
                "Transaction amount exceeds high-value threshold: " + amountStr,
                true,
                false
        );
    }

    public static ProcessingResult rapidSpending(String transactionId, int count, int windowSeconds) {
        return new ProcessingResult(
                transactionId,
                Status.FLAGGED_RAPID_SPENDING,
                String.format("Rapid spending detected: %d transactions within rolling %d seconds", count, windowSeconds),
                false,
                true
        );
    }

    public static ProcessingResult highValueAndRapidSpending(String transactionId, String amountStr, int count, int windowSeconds) {
        return new ProcessingResult(
                transactionId,
                Status.FLAGGED_HIGH_VALUE_AND_RAPID_SPENDING,
                String.format("Both rules triggered! High-value (%s) and Rapid spending (%d in %ds)", amountStr, count, windowSeconds),
                true,
                true
        );
    }

    public static ProcessingResult duplicate(String transactionId) {
        return new ProcessingResult(
                transactionId,
                Status.DUPLICATE_DELIVERY,
                "Duplicate transactionId with identical data safely acknowledged without re-inserting",
                false,
                false
        );
    }

    public static ProcessingResult collision(String transactionId, String details) {
        return new ProcessingResult(
                transactionId,
                Status.ID_COLLISION,
                "Security collision: Transaction ID already exists with conflicting data! " + details,
                false,
                false
        );
    }

    public static ProcessingResult validationError(String transactionId, String reason) {
        return new ProcessingResult(transactionId, Status.REJECTED_VALIDATION_ERROR, reason, false, false);
    }

    public static ProcessingResult malformedJson(String reason) {
        return new ProcessingResult("UNKNOWN", Status.REJECTED_MALFORMED_JSON, reason, false, false);
    }

    public String getTransactionId() {
        return transactionId;
    }

    public Status getStatus() {
        return status;
    }

    public String getReason() {
        return reason;
    }

    public boolean isHighValueFlag() {
        return highValueFlag;
    }

    public boolean isRapidSpendingFlag() {
        return rapidSpendingFlag;
    }

    public Instant getProcessedAt() {
        return processedAt;
    }

    @Override
    public String toString() {
        return "ProcessingResult{" +
                "transactionId='" + transactionId + '\'' +
                ", status=" + status +
                ", reason='" + reason + '\'' +
                ", highValueFlag=" + highValueFlag +
                ", rapidSpendingFlag=" + rapidSpendingFlag +
                ", processedAt=" + processedAt +
                '}';
    }
}
