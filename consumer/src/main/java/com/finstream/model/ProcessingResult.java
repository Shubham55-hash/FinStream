package com.finstream.model;

import java.time.Instant;

/**
 * Encapsulates the outcome of validating and processing a transaction.
 */
public class ProcessingResult {

    public enum Status {
        ACCEPTED,
        FLAGGED_HIGH_VALUE,
        REJECTED_VALIDATION_ERROR,
        REJECTED_MALFORMED_JSON
    }

    private final String transactionId;
    private final Status status;
    private final String reason;
    private final boolean highValueFlag;
    private final Instant processedAt;

    private ProcessingResult(String transactionId, Status status, String reason, boolean highValueFlag) {
        this.transactionId = transactionId;
        this.status = status;
        this.reason = reason;
        this.highValueFlag = highValueFlag;
        this.processedAt = Instant.now();
    }

    public static ProcessingResult accepted(String transactionId) {
        return new ProcessingResult(transactionId, Status.ACCEPTED, "Transaction processed successfully", false);
    }

    public static ProcessingResult highValue(String transactionId, String amountStr) {
        return new ProcessingResult(
                transactionId,
                Status.FLAGGED_HIGH_VALUE,
                "Transaction amount exceeds high-value threshold: " + amountStr,
                true
        );
    }

    public static ProcessingResult validationError(String transactionId, String reason) {
        return new ProcessingResult(transactionId, Status.REJECTED_VALIDATION_ERROR, reason, false);
    }

    public static ProcessingResult malformedJson(String reason) {
        return new ProcessingResult("UNKNOWN", Status.REJECTED_MALFORMED_JSON, reason, false);
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
                ", processedAt=" + processedAt +
                '}';
    }
}
