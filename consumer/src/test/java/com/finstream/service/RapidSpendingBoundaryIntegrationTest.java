package com.finstream.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.finstream.model.ProcessingResult;
import com.finstream.model.TransactionEvent;
import com.finstream.repository.TransactionRepository;
import com.finstream.validation.TransactionValidator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Deterministic test suite verifying rapid-spending window boundaries and persistent idempotency
 * against live MySQL storage using fixed UTC timestamps.
 */
@SpringBootTest
class RapidSpendingBoundaryIntegrationTest {

    @Autowired
    private TransactionRepository repository;

    @Autowired
    private TransactionValidator validator;

    private TransactionProcessingService service;
    private final BigDecimal highValueThreshold = new BigDecimal("50000.00");
    private final int rapidCount = 3;
    private final int rapidWindowSeconds = 60;

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        service = new TransactionProcessingService(
                validator,
                repository,
                objectMapper,
                highValueThreshold,
                rapidCount,
                rapidWindowSeconds
        );
    }

    private TransactionEvent makeEvent(String txId, String accountId, BigDecimal amount, OffsetDateTime timestamp) {
        return new TransactionEvent(txId, accountId, amount, "INR", timestamp, "Boundary Test Store");
    }

    @Test
    void testCase1AndCase2_TwoTransactionsDoNotTrigger_ThirdTriggers() {
        String accountId = "ACC_DET_1_2_" + UUID.randomUUID().toString().substring(0, 6);
        OffsetDateTime baseTime = OffsetDateTime.of(2026, 10, 15, 12, 0, 0, 0, ZoneOffset.UTC);

        // Tx 1 at 12:00:10Z
        String tx1 = "TX_CASE1_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res1 = service.process(makeEvent(tx1, accountId, new BigDecimal("100.00"), baseTime.plusSeconds(10)), null);
        assertEquals(ProcessingResult.Status.ACCEPTED, res1.getStatus());

        // Tx 2 at 12:00:30Z (Case 1: Two transactions inside 60s do NOT trigger)
        String tx2 = "TX_CASE1_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res2 = service.process(makeEvent(tx2, accountId, new BigDecimal("200.00"), baseTime.plusSeconds(30)), null);
        assertEquals(ProcessingResult.Status.ACCEPTED, res2.getStatus());
        assertFalse(res2.isRapidSpendingFlag(), "Two transactions must not trigger rapid spending");

        // Tx 3 at 12:00:50Z (Case 2: Third transaction within window triggers rule)
        String tx3 = "TX_CASE2_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res3 = service.process(makeEvent(tx3, accountId, new BigDecimal("300.00"), baseTime.plusSeconds(50)), null);
        assertEquals(ProcessingResult.Status.FLAGGED_RAPID_SPENDING, res3.getStatus());
        assertTrue(res3.isRapidSpendingFlag(), "Three transactions within window must trigger rapid spending");
    }

    @Test
    void testCase3_Exact60SecondBoundaryIncluded() {
        String accountId = "ACC_EXACT_60_" + UUID.randomUUID().toString().substring(0, 6);
        OffsetDateTime t0 = OffsetDateTime.of(2026, 10, 15, 12, 0, 0, 0, ZoneOffset.UTC);

        // Tx 1 at exactly 12:00:00Z
        String tx1 = "TX_EXACT_01_" + UUID.randomUUID().toString().substring(0, 6);
        service.process(makeEvent(tx1, accountId, new BigDecimal("100.00"), t0), null);

        // Tx 2 at 12:00:30Z
        String tx2 = "TX_EXACT_02_" + UUID.randomUUID().toString().substring(0, 6);
        service.process(makeEvent(tx2, accountId, new BigDecimal("100.00"), t0.plusSeconds(30)), null);

        // Tx 3 at exactly 12:01:00Z (exactly 60s after Tx 1)
        // Window [t - 60s, t] is [12:00:00, 12:01:00]. Tx 1 is at the exact lower boundary.
        String tx3 = "TX_EXACT_03_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res3 = service.process(makeEvent(tx3, accountId, new BigDecimal("100.00"), t0.plusSeconds(60)), null);

        assertEquals(ProcessingResult.Status.FLAGGED_RAPID_SPENDING, res3.getStatus(),
                "Exact 60-second boundary must be inclusive and trigger rapid spending");
    }

    @Test
    void testCase4_OutsideWindowExcluded() {
        String accountId = "ACC_OUTSIDE_60_" + UUID.randomUUID().toString().substring(0, 6);
        OffsetDateTime t0 = OffsetDateTime.of(2026, 10, 15, 12, 0, 0, 0, ZoneOffset.UTC);

        // Tx 1 at 11:59:59Z (61 seconds before Tx 3)
        String tx1 = "TX_OUT_01_" + UUID.randomUUID().toString().substring(0, 6);
        service.process(makeEvent(tx1, accountId, new BigDecimal("100.00"), t0.minusSeconds(1)), null);

        // Tx 2 at 12:00:30Z
        String tx2 = "TX_OUT_02_" + UUID.randomUUID().toString().substring(0, 6);
        service.process(makeEvent(tx2, accountId, new BigDecimal("100.00"), t0.plusSeconds(30)), null);

        // Tx 3 at 12:01:00Z (Window is [12:00:00, 12:01:00]; Tx 1 is at 11:59:59, so it is outside)
        String tx3 = "TX_OUT_03_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res3 = service.process(makeEvent(tx3, accountId, new BigDecimal("100.00"), t0.plusSeconds(60)), null);

        assertEquals(ProcessingResult.Status.ACCEPTED, res3.getStatus(),
                "Transaction older than 60s boundary must be excluded; total count is only 2");
        assertFalse(res3.isRapidSpendingFlag());
    }

    @Test
    void testCase5_DuplicateEventDoesNotIncreaseCount() {
        String accountId = "ACC_DUPE_CNT_" + UUID.randomUUID().toString().substring(0, 6);
        OffsetDateTime t0 = OffsetDateTime.of(2026, 10, 15, 12, 0, 0, 0, ZoneOffset.UTC);

        String tx1 = "TX_DUPE_C5_" + UUID.randomUUID().toString().substring(0, 6);
        service.process(makeEvent(tx1, accountId, new BigDecimal("100.00"), t0), null);

        // Replay tx1
        ProcessingResult dupeRes = service.process(makeEvent(tx1, accountId, new BigDecimal("100.00"), t0), null);
        assertEquals(ProcessingResult.Status.DUPLICATE_DELIVERY, dupeRes.getStatus());

        // Send tx2 at 12:00:30Z
        String tx2 = "TX_DUPE_C5_B_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res2 = service.process(makeEvent(tx2, accountId, new BigDecimal("100.00"), t0.plusSeconds(30)), null);

        // Count in window must be 2 (tx1 and tx2), NOT 3!
        assertEquals(ProcessingResult.Status.ACCEPTED, res2.getStatus(),
                "Duplicate replay must not increase distinct transaction count");
        assertFalse(res2.isRapidSpendingFlag());
    }

    @Test
    void testCase6_InvalidTransactionDoesNotCount() {
        String accountId = "ACC_INVALID_CNT_" + UUID.randomUUID().toString().substring(0, 6);
        OffsetDateTime t0 = OffsetDateTime.of(2026, 10, 15, 12, 0, 0, 0, ZoneOffset.UTC);

        // Tx 1 valid at 12:00:10Z
        String tx1 = "TX_INV_01_" + UUID.randomUUID().toString().substring(0, 6);
        service.process(makeEvent(tx1, accountId, new BigDecimal("100.00"), t0.plusSeconds(10)), null);

        // Tx 2 invalid (negative amount) at 12:00:20Z
        String tx2 = "TX_INV_02_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res2 = service.process(makeEvent(tx2, accountId, new BigDecimal("-50.00"), t0.plusSeconds(20)), null);
        assertEquals(ProcessingResult.Status.REJECTED_VALIDATION_ERROR, res2.getStatus());

        // Tx 3 valid at 12:00:30Z
        String tx3 = "TX_INV_03_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res3 = service.process(makeEvent(tx3, accountId, new BigDecimal("100.00"), t0.plusSeconds(30)), null);

        // Total valid transactions is only 2
        assertEquals(ProcessingResult.Status.ACCEPTED, res3.getStatus(),
                "Invalid transactions must not be counted toward rapid-spending");
        assertFalse(res3.isRapidSpendingFlag());
    }

    @Test
    void testCase7_OutOfOrderEventBehavior() {
        String accountId = "ACC_OUT_OF_ORDER_" + UUID.randomUUID().toString().substring(0, 6);
        OffsetDateTime t0 = OffsetDateTime.of(2026, 10, 15, 12, 0, 0, 0, ZoneOffset.UTC);

        // Deliver Tx 2 at 12:00:20Z first
        String tx2 = "TX_OOO_02_" + UUID.randomUUID().toString().substring(0, 6);
        service.process(makeEvent(tx2, accountId, new BigDecimal("100.00"), t0.plusSeconds(20)), null);

        // Deliver Tx 3 at 12:00:40Z second (only 2 in window so far)
        String tx3 = "TX_OOO_03_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res3 = service.process(makeEvent(tx3, accountId, new BigDecimal("100.00"), t0.plusSeconds(40)), null);
        assertEquals(ProcessingResult.Status.ACCEPTED, res3.getStatus());

        // Deliver Tx 1 at 12:00:05Z third (out-of-order, earlier event time)
        String tx1 = "TX_OOO_01_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res1 = service.process(makeEvent(tx1, accountId, new BigDecimal("100.00"), t0.plusSeconds(5)), null);
        // Window [t - 60s, t] for Tx 1 is [11:59:05, 12:00:05]. No transactions exist in that window yet.
        assertEquals(ProcessingResult.Status.ACCEPTED, res1.getStatus());

        // Now, deliver Tx 4 at 12:00:50Z (now all 3 prior transactions: Tx 1, 2, 3 are in [11:59:50, 12:00:50])
        String tx4 = "TX_OOO_04_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res4 = service.process(makeEvent(tx4, accountId, new BigDecimal("100.00"), t0.plusSeconds(50)), null);
        assertEquals(ProcessingResult.Status.FLAGGED_RAPID_SPENDING, res4.getStatus(),
                "Subsequent transactions properly account for out-of-order persisted events in their rolling window");
    }

    @Test
    void testCase8_RestartSimulatedEvaluatesPersistedHistory() {
        String accountId = "ACC_RESTART_" + UUID.randomUUID().toString().substring(0, 6);
        OffsetDateTime t0 = OffsetDateTime.of(2026, 10, 15, 12, 0, 0, 0, ZoneOffset.UTC);

        // Service instance 1 processes Tx 1 and Tx 2
        String tx1 = "TX_RST_01_" + UUID.randomUUID().toString().substring(0, 6);
        service.process(makeEvent(tx1, accountId, new BigDecimal("100.00"), t0.plusSeconds(10)), null);

        String tx2 = "TX_RST_02_" + UUID.randomUUID().toString().substring(0, 6);
        service.process(makeEvent(tx2, accountId, new BigDecimal("100.00"), t0.plusSeconds(25)), null);

        // Simulate complete application restart by instantiating a fresh service instance
        ObjectMapper newObjectMapper = new ObjectMapper();
        newObjectMapper.registerModule(new JavaTimeModule());
        TransactionProcessingService restartedService = new TransactionProcessingService(
                validator,
                repository,
                newObjectMapper,
                highValueThreshold,
                rapidCount,
                rapidWindowSeconds
        );

        // Process Tx 3 with the restarted service
        String tx3 = "TX_RST_03_" + UUID.randomUUID().toString().substring(0, 6);
        ProcessingResult res3 = restartedService.process(makeEvent(tx3, accountId, new BigDecimal("100.00"), t0.plusSeconds(40)), null);

        assertEquals(ProcessingResult.Status.FLAGGED_RAPID_SPENDING, res3.getStatus(),
                "Restarted service must query MySQL to find prior transactions and trigger rapid spending");
        assertTrue(res3.isRapidSpendingFlag());
    }
}
