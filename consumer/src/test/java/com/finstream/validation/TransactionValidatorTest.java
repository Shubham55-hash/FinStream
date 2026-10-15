package com.finstream.validation;

import com.finstream.exception.TransactionValidationException;
import com.finstream.model.TransactionEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;

class TransactionValidatorTest {

    private TransactionValidator validator;

    @BeforeEach
    void setUp() {
        validator = new TransactionValidator();
    }

    private TransactionEvent createValidEvent() {
        return new TransactionEvent(
                "TX1001",
                "ACC101",
                new BigDecimal("2500.00"),
                "INR",
                OffsetDateTime.parse("2026-10-15T12:30:00Z"),
                "Demo Store"
        );
    }

    @Test
    void testValidTransactionPasses() {
        TransactionEvent event = createValidEvent();
        assertDoesNotThrow(() -> validator.validate(event));
    }

    @Test
    void testNullEventThrows() {
        TransactionValidationException ex = assertThrows(
                TransactionValidationException.class,
                () -> validator.validate(null)
        );
        assertTrue(ex.getMessage().contains("cannot be null"));
    }

    @Test
    void testBlankTransactionIdThrows() {
        TransactionEvent event = createValidEvent();
        event.setTransactionId("   ");
        TransactionValidationException ex = assertThrows(
                TransactionValidationException.class,
                () -> validator.validate(event)
        );
        assertTrue(ex.getMessage().contains("transactionId must be a non-empty string"));
    }

    @Test
    void testBlankAccountIdThrows() {
        TransactionEvent event = createValidEvent();
        event.setAccountId("");
        TransactionValidationException ex = assertThrows(
                TransactionValidationException.class,
                () -> validator.validate(event)
        );
        assertTrue(ex.getMessage().contains("accountId must be a non-empty string"));
    }

    @Test
    void testBlankMerchantThrows() {
        TransactionEvent event = createValidEvent();
        event.setMerchant(null);
        TransactionValidationException ex = assertThrows(
                TransactionValidationException.class,
                () -> validator.validate(event)
        );
        assertTrue(ex.getMessage().contains("merchant must be a non-empty string"));
    }

    @Test
    void testNonPositiveAmountThrows() {
        TransactionEvent zeroEvent = createValidEvent();
        zeroEvent.setAmount(BigDecimal.ZERO);
        assertThrows(TransactionValidationException.class, () -> validator.validate(zeroEvent));

        TransactionEvent negativeEvent = createValidEvent();
        negativeEvent.setAmount(new BigDecimal("-10.50"));
        assertThrows(TransactionValidationException.class, () -> validator.validate(negativeEvent));
    }

    @Test
    void testUnsupportedCurrencyThrows() {
        TransactionEvent event = createValidEvent();
        event.setCurrency("USD");
        TransactionValidationException ex = assertThrows(
                TransactionValidationException.class,
                () -> validator.validate(event)
        );
        assertTrue(ex.getMessage().contains("Unsupported or invalid currency"));
    }

    @Test
    void testNullTimestampThrows() {
        TransactionEvent event = createValidEvent();
        event.setTimestamp(null);
        TransactionValidationException ex = assertThrows(
                TransactionValidationException.class,
                () -> validator.validate(event)
        );
        assertTrue(ex.getMessage().contains("timestamp cannot be null"));
    }
}
