package com.finstream.validation;

import com.finstream.exception.TransactionValidationException;
import com.finstream.model.TransactionEvent;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.Set;

/**
 * Validates incoming transaction events according to schema and business rules.
 */
@Component
public class TransactionValidator {

    private static final Set<String> SUPPORTED_CURRENCIES = Set.of("INR");

    /**
     * Validates all required fields and value constraints on a TransactionEvent.
     *
     * @param event the transaction event to validate
     * @throws TransactionValidationException if any constraint is violated
     */
    public void validate(TransactionEvent event) {
        if (event == null) {
            throw new TransactionValidationException("Transaction event payload cannot be null");
        }

        String txId = event.getTransactionId();
        if (txId == null || txId.trim().isEmpty()) {
            throw new TransactionValidationException(txId, "transactionId must be a non-empty string");
        }

        if (event.getAccountId() == null || event.getAccountId().trim().isEmpty()) {
            throw new TransactionValidationException(txId, "accountId must be a non-empty string");
        }

        if (event.getMerchant() == null || event.getMerchant().trim().isEmpty()) {
            throw new TransactionValidationException(txId, "merchant must be a non-empty string");
        }

        BigDecimal amount = event.getAmount();
        if (amount == null) {
            throw new TransactionValidationException(txId, "amount cannot be null");
        }
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new TransactionValidationException(txId, "amount must be a positive number, got: " + amount);
        }

        if (event.getCurrency() == null || !SUPPORTED_CURRENCIES.contains(event.getCurrency())) {
            throw new TransactionValidationException(
                    txId,
                    "Unsupported or invalid currency '" + event.getCurrency() + "'. Supported: " + SUPPORTED_CURRENCIES
            );
        }

        if (event.getTimestamp() == null) {
            throw new TransactionValidationException(txId, "timestamp cannot be null and must be a valid ISO-8601 UTC timestamp");
        }
    }
}
