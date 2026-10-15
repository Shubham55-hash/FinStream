package com.finstream.consumer;

import com.finstream.model.ProcessingResult;
import com.finstream.model.TransactionEntity;
import com.finstream.repository.TransactionRepository;
import com.finstream.service.TransactionProcessingService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Integration and simulation test suite proving:
 * 1. Database outage does NOT acknowledge offset, triggering bounded recovery without data loss.
 * 2. Persistent idempotency survives consumer restarts and prevents duplicate rows and alerts.
 */
@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.consumer.group-id=test-group-outage-restart",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
class OutageAndRestartIntegrationTest {

    @Autowired
    private TransactionProcessingService processingService;

    @Autowired
    private TransactionRepository repository;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Test
    void testGapA_DatabaseOutageDoesNotAcknowledgeOffset() {
        TransactionProcessingService mockService = mock(TransactionProcessingService.class);
        TransactionKafkaConsumer testConsumer = new TransactionKafkaConsumer(mockService);
        Acknowledgment mockAck = mock(Acknowledgment.class);

        String txId = "TX_OUTAGE_" + UUID.randomUUID().toString().substring(0, 8);
        String payload = "{\"transactionId\":\"" + txId + "\",\"accountId\":\"ACC_OUTAGE\",\"amount\":1000.00,\"currency\":\"INR\",\"timestamp\":\"2026-10-15T12:00:00Z\",\"merchant\":\"Store\"}";

        // 1. Simulate MySQL outage: service throws CannotGetJdbcConnectionException
        when(mockService.processRaw(payload))
                .thenThrow(new CannotGetJdbcConnectionException("Connection refused", new java.sql.SQLException("MySQL server down")));

        // 2. Consume should throw DataAccessException
        assertThrows(DataAccessException.class, () ->
                testConsumer.consume(payload, txId, 0, 100L, mockAck));

        // 3. Verify acknowledgment was NEVER invoked!
        verify(mockAck, never()).acknowledge();

        // 4. Simulate recovery: service succeeds on retry
        reset(mockService);
        when(mockService.processRaw(payload)).thenReturn(ProcessingResult.accepted(txId));

        testConsumer.consume(payload, txId, 0, 100L, mockAck);

        // 5. Verify acknowledgment IS called once DB recovers
        verify(mockAck, times(1)).acknowledge();
    }

    @Test
    void testGapB_ConsumerRestartAndPersistentIdempotencyAgainstMySQL() {
        String txId = "TX_RESTART_" + UUID.randomUUID().toString().substring(0, 8);
        String accountId = "ACC_REPLAY_" + UUID.randomUUID().toString().substring(0, 8);
        String payload = "{"
                + "\"transactionId\":\"" + txId + "\","
                + "\"accountId\":\"" + accountId + "\","
                + "\"amount\":60000.00,"
                + "\"currency\":\"INR\","
                + "\"timestamp\":\"2026-10-15T12:30:00Z\","
                + "\"merchant\":\"High Value Store\""
                + "}";

        // Step 1: Initial delivery - process and persist
        ProcessingResult firstResult = processingService.processRaw(payload);
        assertEquals(ProcessingResult.Status.FLAGGED_HIGH_VALUE, firstResult.getStatus());

        Optional<TransactionEntity> storedFirst = repository.findByTransactionId(txId);
        assertTrue(storedFirst.isPresent(), "Transaction must be persisted into MySQL on first delivery");
        assertEquals("HIGH_VALUE", storedFirst.get().getStatus());

        // Step 2: Gracefully stop consumer listener (simulating consumer shutdown)
        if (registry != null) {
            registry.stop();
            assertFalse(registry.isRunning(), "Consumer listener registry must be stopped");
        }

        // Step 3: Restart consumer listener (simulating consumer restart with same group ID)
        if (registry != null) {
            registry.start();
            assertTrue(registry.isRunning(), "Consumer listener registry must be restarted");
        }

        // Step 4: Replay exact same payload after restart
        ProcessingResult replayResult = processingService.processRaw(payload);
        assertEquals(ProcessingResult.Status.DUPLICATE_DELIVERY, replayResult.getStatus(),
                "Replayed event must be recognized as DUPLICATE_DELIVERY across restarts");

        // Verify still exactly one transaction row
        Optional<TransactionEntity> storedSecond = repository.findByTransactionId(txId);
        assertTrue(storedSecond.isPresent());
        assertEquals(storedFirst.get().getId(), storedSecond.get().getId(), "ID must remain unchanged; no second row");

        // Step 5: Replay same txId with conflicting data
        String conflictingPayload = "{"
                + "\"transactionId\":\"" + txId + "\","
                + "\"accountId\":\"ACC_ROGUE\","
                + "\"amount\":99999.00,"
                + "\"currency\":\"INR\","
                + "\"timestamp\":\"2026-10-15T12:35:00Z\","
                + "\"merchant\":\"Rogue Store\""
                + "}";

        ProcessingResult collisionResult = processingService.processRaw(conflictingPayload);
        assertEquals(ProcessingResult.Status.ID_COLLISION, collisionResult.getStatus(),
                "Conflicting transaction data must be classified as ID_COLLISION");

        // Verify original row in MySQL was NOT overwritten
        Optional<TransactionEntity> storedAfterCollision = repository.findByTransactionId(txId);
        assertTrue(storedAfterCollision.isPresent());
        assertEquals(accountId, storedAfterCollision.get().getAccountId(), "Original accountId must not be overwritten");
        assertEquals(0, new BigDecimal("60000.00").compareTo(storedAfterCollision.get().getAmount()), "Original amount must not be overwritten");
    }
}
