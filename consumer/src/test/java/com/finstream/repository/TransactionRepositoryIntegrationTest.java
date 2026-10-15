package com.finstream.repository;

import com.finstream.model.TransactionEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DuplicateKeyException;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class TransactionRepositoryIntegrationTest {

    @Autowired
    private TransactionRepository repository;

    @Test
    void testSaveAndFindTransactionWithExactDecimalAndUtcTimestamp() {
        String txId = "TX_REPO_TEST_" + UUID.randomUUID().toString().substring(0, 8);
        OffsetDateTime timestamp = OffsetDateTime.of(2026, 10, 15, 14, 30, 45, 0, ZoneOffset.UTC);
        BigDecimal amount = new BigDecimal("12345.67");

        TransactionEntity entity = new TransactionEntity(
                txId,
                "ACC_REPO_01",
                amount,
                "INR",
                timestamp,
                "Integration Store",
                "ACCEPTED"
        );

        repository.saveTransaction(entity);

        Optional<TransactionEntity> found = repository.findByTransactionId(txId);
        assertTrue(found.isPresent(), "Transaction should be retrieved from MySQL");
        assertEquals(txId, found.get().getTransactionId());
        assertEquals("ACC_REPO_01", found.get().getAccountId());
        assertEquals(0, amount.compareTo(found.get().getAmount()), "Decimal precision must be exact");
        assertEquals("INR", found.get().getCurrency());
        assertEquals("ACCEPTED", found.get().getStatus());
        assertNotNull(found.get().getCreatedAt());
    }

    @Test
    void testUniqueConstraintEnforcesNoDuplicateTransactionIds() {
        String txId = "TX_UNIQUE_TEST_" + UUID.randomUUID().toString().substring(0, 8);
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);

        TransactionEntity entity1 = new TransactionEntity(txId, "ACC_1", new BigDecimal("100.00"), "INR", now, "Store", "ACCEPTED");
        repository.saveTransaction(entity1);

        TransactionEntity entity2 = new TransactionEntity(txId, "ACC_2", new BigDecimal("200.00"), "INR", now, "Store", "ACCEPTED");
        assertThrows(DuplicateKeyException.class, () -> repository.saveTransaction(entity2),
                "MySQL unique constraint must prevent inserting duplicate transaction_id");
    }

    @Test
    void testCountTransactionsInWindowRollingCalculation() {
        String accountId = "ACC_WINDOW_TEST_" + UUID.randomUUID().toString().substring(0, 6);
        OffsetDateTime baseTime = OffsetDateTime.of(2026, 10, 15, 10, 0, 0, 0, ZoneOffset.UTC);

        // Insert tx at t - 30s
        repository.saveTransaction(new TransactionEntity(
                "TX_W_01_" + UUID.randomUUID().toString().substring(0, 6),
                accountId, new BigDecimal("100.00"), "INR", baseTime.minusSeconds(30), "Store", "ACCEPTED"
        ));

        // Insert tx at t - 10s
        repository.saveTransaction(new TransactionEntity(
                "TX_W_02_" + UUID.randomUUID().toString().substring(0, 6),
                accountId, new BigDecimal("200.00"), "INR", baseTime.minusSeconds(10), "Store", "ACCEPTED"
        ));

        // Insert tx at t - 70s (outside the 60-second window!)
        repository.saveTransaction(new TransactionEntity(
                "TX_W_03_" + UUID.randomUUID().toString().substring(0, 6),
                accountId, new BigDecimal("300.00"), "INR", baseTime.minusSeconds(70), "Store", "ACCEPTED"
        ));

        // Query window [baseTime - 60s, baseTime]
        int count = repository.countTransactionsInWindow(accountId, baseTime.minusSeconds(60), baseTime);

        assertEquals(2, count, "Only the 2 transactions within the rolling 60s window should be counted");
    }

    @Test
    void testSaveAlertAndRejectedEvents() {
        String txId = "TX_ALERT_TEST_" + UUID.randomUUID().toString().substring(0, 8);

        assertDoesNotThrow(() ->
                repository.saveAlert(txId, "ACC_ALERT_01", "HIGH_VALUE", "Amount > 50000 INR"));

        assertDoesNotThrow(() ->
                repository.saveRejectedEvent(txId, "{ \"bad\": true }", "Validation failure", "VALIDATION_ERROR"));
    }
}
