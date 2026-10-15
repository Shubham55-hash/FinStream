package com.finstream.exception;

/**
 * Thrown when a TransactionEvent violates schema or business rules.
 */
public class TransactionValidationException extends RuntimeException {

    private final String transactionId;

    public TransactionValidationException(String message) {
        super(message);
        this.transactionId = "UNKNOWN";
    }

    public TransactionValidationException(String transactionId, String message) {
        super(message);
        this.transactionId = transactionId;
    }

    public String getTransactionId() {
        return transactionId;
    }
}
