package com.finstream.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.finstream.model.ProcessingResult;
import com.finstream.model.TransactionEntity;
import com.finstream.model.TransactionEvent;
import com.finstream.repository.TransactionRepository;
import com.finstream.validation.TransactionValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.TransientDataAccessResourceException;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TransactionProcessingServiceTest {

    @Mock
    private TransactionRepository repository;

    private TransactionValidator validator;
    private ObjectMapper objectMapper;
    private TransactionProcessingService service;

    private final BigDecimal threshold = new BigDecimal("50000.00");
    private final int rapidCount = 3;
    private final int rapidWindowSeconds = 60;

    @BeforeEach
    void setUp() {
        validator = new TransactionValidator();
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        service = new TransactionProcessingService(
                validator,
                repository,
                objectMapper,
                threshold,
                rapidCount,
                rapidWindowSeconds
        );
    }

    private TransactionEvent createEvent(String txId, String accountId, BigDecimal amount, OffsetDateTime timestamp) {
        return new TransactionEvent(
                txId,
                accountId,
                amount,
                "INR",
                timestamp,
                "Demo Store"
        );
    }

    @Test
    void testProcessNormalTransaction() {
        OffsetDateTime now = OffsetDateTime.parse("2026-10-15T12:30:00Z");
        TransactionEvent event = createEvent("TX1001", "ACC101", new BigDecimal("2500.00"), now);

        when(repository.findByTransactionId("TX1001")).thenReturn(Optional.empty());
        when(repository.countTransactionsInWindow(eq("ACC101"), any(), any())).thenReturn(0);

        ProcessingResult result = service.process(event, null);

        assertEquals("TX1001", result.getTransactionId());
        assertEquals(ProcessingResult.Status.ACCEPTED, result.getStatus());
        assertFalse(result.isHighValueFlag());
        assertFalse(result.isRapidSpendingFlag());

        verify(repository).saveTransaction(any(TransactionEntity.class));
        verify(repository, never()).saveAlert(any(), any(), any(), any());
    }

    @Test
    void testProcessHighValueTransactionStrictlyGreaterThanThreshold() {
        OffsetDateTime now = OffsetDateTime.parse("2026-10-15T12:30:00Z");
        TransactionEvent event = createEvent("TX1002", "ACC101", new BigDecimal("50000.01"), now);

        when(repository.findByTransactionId("TX1002")).thenReturn(Optional.empty());
        when(repository.countTransactionsInWindow(eq("ACC101"), any(), any())).thenReturn(0);

        ProcessingResult result = service.process(event, null);

        assertEquals(ProcessingResult.Status.FLAGGED_HIGH_VALUE, result.getStatus());
        assertTrue(result.isHighValueFlag());

        verify(repository).saveTransaction(argThat(entity -> "HIGH_VALUE".equals(entity.getStatus())));
        verify(repository).saveAlert(eq("TX1002"), eq("ACC101"), eq("HIGH_VALUE"), anyString());
    }

    @Test
    void testProcessBoundaryTransactionAtExactlyThresholdDoesNotTrigger() {
        OffsetDateTime now = OffsetDateTime.parse("2026-10-15T12:30:00Z");
        TransactionEvent event = createEvent("TX1003", "ACC101", new BigDecimal("50000.00"), now);

        when(repository.findByTransactionId("TX1003")).thenReturn(Optional.empty());
        when(repository.countTransactionsInWindow(eq("ACC101"), any(), any())).thenReturn(0);

        ProcessingResult result = service.process(event, null);

        assertEquals(ProcessingResult.Status.ACCEPTED, result.getStatus());
        assertFalse(result.isHighValueFlag());
        verify(repository, never()).saveAlert(any(), any(), eq("HIGH_VALUE"), any());
    }

    @Test
    void testProcessDuplicateDeliveryWithIdenticalData() {
        OffsetDateTime ts = OffsetDateTime.parse("2026-10-15T12:30:00Z");
        TransactionEvent event = createEvent("TX_DUPE", "ACC101", new BigDecimal("1000.00"), ts);

        TransactionEntity stored = new TransactionEntity("TX_DUPE", "ACC101", new BigDecimal("1000.00"), "INR", ts, "Demo Store", "ACCEPTED");
        when(repository.findByTransactionId("TX_DUPE")).thenReturn(Optional.of(stored));

        ProcessingResult result = service.process(event, null);

        assertEquals(ProcessingResult.Status.DUPLICATE_DELIVERY, result.getStatus());
        verify(repository, never()).saveTransaction(any());
        verify(repository, never()).saveAlert(any(), any(), any(), any());
    }

    @Test
    void testProcessIdCollisionWithConflictingData() {
        OffsetDateTime ts = OffsetDateTime.parse("2026-10-15T12:30:00Z");
        TransactionEvent event = createEvent("TX_COLLISION", "ACC_NEW", new BigDecimal("9999.00"), ts);

        // Stored has different account and amount
        TransactionEntity stored = new TransactionEntity("TX_COLLISION", "ACC_OLD", new BigDecimal("100.00"), "INR", ts, "Demo Store", "ACCEPTED");
        when(repository.findByTransactionId("TX_COLLISION")).thenReturn(Optional.of(stored));

        ProcessingResult result = service.process(event, "raw-json-payload");

        assertEquals(ProcessingResult.Status.ID_COLLISION, result.getStatus());
        verify(repository, never()).saveTransaction(any());
        verify(repository).saveAlert(eq("TX_COLLISION"), eq("ACC_NEW"), eq("ID_COLLISION"), anyString());
        verify(repository).saveRejectedEvent(eq("TX_COLLISION"), any(), any(), eq("ID_COLLISION"));
    }

    @Test
    void testRapidSpendingRuleTriggeredOnThirdTransactionInWindow() {
        OffsetDateTime now = OffsetDateTime.parse("2026-10-15T12:30:30Z");
        TransactionEvent event = createEvent("TX_RAPID_03", "ACC101", new BigDecimal("500.00"), now);

        when(repository.findByTransactionId("TX_RAPID_03")).thenReturn(Optional.empty());
        // 2 prior transactions already exist in the [now - 60s, now] window
        when(repository.countTransactionsInWindow(eq("ACC101"), any(), any())).thenReturn(2);

        ProcessingResult result = service.process(event, null);

        assertEquals(ProcessingResult.Status.FLAGGED_RAPID_SPENDING, result.getStatus());
        assertTrue(result.isRapidSpendingFlag());
        assertFalse(result.isHighValueFlag());

        verify(repository).saveTransaction(argThat(entity -> "RAPID_SPENDING".equals(entity.getStatus())));
        verify(repository).saveAlert(eq("TX_RAPID_03"), eq("ACC101"), eq("RAPID_SPENDING"), contains("reached 3 transactions"));
    }

    @Test
    void testTwoTransactionsInWindowDoNotTriggerRapidSpending() {
        OffsetDateTime now = OffsetDateTime.parse("2026-10-15T12:30:30Z");
        TransactionEvent event = createEvent("TX_RAPID_02", "ACC101", new BigDecimal("500.00"), now);

        when(repository.findByTransactionId("TX_RAPID_02")).thenReturn(Optional.empty());
        // Only 1 prior transaction in window -> total becomes 2 (below 3)
        when(repository.countTransactionsInWindow(eq("ACC101"), any(), any())).thenReturn(1);

        ProcessingResult result = service.process(event, null);

        assertEquals(ProcessingResult.Status.ACCEPTED, result.getStatus());
        assertFalse(result.isRapidSpendingFlag());
        verify(repository, never()).saveAlert(any(), any(), eq("RAPID_SPENDING"), any());
    }

    @Test
    void testBothRulesTriggerSimultaneously() {
        OffsetDateTime now = OffsetDateTime.parse("2026-10-15T12:30:30Z");
        // High amount AND rapid spending
        TransactionEvent event = createEvent("TX_DUAL", "ACC101", new BigDecimal("75000.00"), now);

        when(repository.findByTransactionId("TX_DUAL")).thenReturn(Optional.empty());
        when(repository.countTransactionsInWindow(eq("ACC101"), any(), any())).thenReturn(2);

        ProcessingResult result = service.process(event, null);

        assertEquals(ProcessingResult.Status.FLAGGED_HIGH_VALUE_AND_RAPID_SPENDING, result.getStatus());
        assertTrue(result.isHighValueFlag());
        assertTrue(result.isRapidSpendingFlag());

        verify(repository).saveTransaction(argThat(entity -> "HIGH_VALUE_AND_RAPID_SPENDING".equals(entity.getStatus())));
        verify(repository).saveAlert(eq("TX_DUAL"), eq("ACC101"), eq("HIGH_VALUE"), anyString());
        verify(repository).saveAlert(eq("TX_DUAL"), eq("ACC101"), eq("RAPID_SPENDING"), anyString());
    }

    @Test
    void testDatabaseFailureThrowsDataAccessException() {
        OffsetDateTime now = OffsetDateTime.parse("2026-10-15T12:30:00Z");
        TransactionEvent event = createEvent("TX_FAIL", "ACC101", new BigDecimal("100.00"), now);

        when(repository.findByTransactionId("TX_FAIL")).thenThrow(new TransientDataAccessResourceException("MySQL connection refused"));

        assertThrows(DataAccessException.class, () -> service.process(event, null));
    }
}
