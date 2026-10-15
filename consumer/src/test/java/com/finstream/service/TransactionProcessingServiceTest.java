package com.finstream.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.finstream.model.ProcessingResult;
import com.finstream.model.TransactionEvent;
import com.finstream.validation.TransactionValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;

class TransactionProcessingServiceTest {

    private TransactionProcessingService service;
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        TransactionValidator validator = new TransactionValidator();
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        service = new TransactionProcessingService(validator, objectMapper, new BigDecimal("50000.00"));
    }

    private TransactionEvent createEvent(String txId, BigDecimal amount) {
        return new TransactionEvent(
                txId,
                "ACC101",
                amount,
                "INR",
                OffsetDateTime.parse("2026-10-15T12:30:00Z"),
                "Demo Store"
        );
    }

    @Test
    void testProcessValidNormalTransaction() {
        TransactionEvent event = createEvent("TX1001", new BigDecimal("2500.00"));
        ProcessingResult result = service.process(event);

        assertEquals("TX1001", result.getTransactionId());
        assertEquals(ProcessingResult.Status.ACCEPTED, result.getStatus());
        assertFalse(result.isHighValueFlag());
    }

    @Test
    void testProcessHighValueTransaction() {
        TransactionEvent event = createEvent("TX1002", new BigDecimal("75000.50"));
        ProcessingResult result = service.process(event);

        assertEquals("TX1002", result.getTransactionId());
        assertEquals(ProcessingResult.Status.FLAGGED_HIGH_VALUE, result.getStatus());
        assertTrue(result.isHighValueFlag());
    }

    @Test
    void testProcessBoundaryTransactionAtThreshold() {
        // At exactly 50000.00, it is not strictly greater than 50000.00
        TransactionEvent event = createEvent("TX1003", new BigDecimal("50000.00"));
        ProcessingResult result = service.process(event);

        assertEquals(ProcessingResult.Status.ACCEPTED, result.getStatus());
        assertFalse(result.isHighValueFlag());
    }

    @Test
    void testProcessRawValidJson() {
        String json = "{"
                + "\"transactionId\":\"TX1004\","
                + "\"accountId\":\"ACC102\","
                + "\"amount\":1500.00,"
                + "\"currency\":\"INR\","
                + "\"timestamp\":\"2026-10-15T12:30:00Z\","
                + "\"merchant\":\"Flipkart\""
                + "}";

        ProcessingResult result = service.processRaw(json);
        assertEquals("TX1004", result.getTransactionId());
        assertEquals(ProcessingResult.Status.ACCEPTED, result.getStatus());
    }

    @Test
    void testProcessMalformedJsonDoesNotCrash() {
        String badJson = "{ invalid-json-payload ";
        ProcessingResult result = service.processRaw(badJson);

        assertEquals("UNKNOWN", result.getTransactionId());
        assertEquals(ProcessingResult.Status.REJECTED_MALFORMED_JSON, result.getStatus());
        assertTrue(result.getReason().contains("JSON parsing error"));
    }

    @Test
    void testProcessValidationErrorHandledGracefully() {
        String invalidDataJson = "{"
                + "\"transactionId\":\"TX1005\","
                + "\"accountId\":\"ACC103\","
                + "\"amount\":-20.00,"
                + "\"currency\":\"INR\","
                + "\"timestamp\":\"2026-10-15T12:30:00Z\","
                + "\"merchant\":\"Demo Store\""
                + "}";

        ProcessingResult result = service.processRaw(invalidDataJson);
        assertEquals("TX1005", result.getTransactionId());
        assertEquals(ProcessingResult.Status.REJECTED_VALIDATION_ERROR, result.getStatus());
        assertTrue(result.getReason().contains("amount must be a positive number"));
    }

    @Test
    void testProcessDuplicateTracking() {
        TransactionEvent event1 = createEvent("TX_DUPE_01", new BigDecimal("1000.00"));
        TransactionEvent event2 = createEvent("TX_DUPE_01", new BigDecimal("1000.00"));

        service.process(event1);
        assertEquals(0, service.getDuplicateCount());

        service.process(event2);
        assertEquals(1, service.getDuplicateCount());
    }
}
