package com.finstream.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.finstream.repository.TransactionRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * REST API Controller serving the live FinStream web dashboard.
 * Provides live metrics, recent transactions/alerts feeds, and manual transaction triggering.
 */
@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class DashboardApiController {

    private static final Logger logger = LoggerFactory.getLogger(DashboardApiController.class);
    private static final AtomicLong txSequence = new AtomicLong(System.currentTimeMillis() % 100000);

    private final TransactionRepository transactionRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${finstream.kafka.topic:transactions}")
    private String topic;

    @Autowired
    public DashboardApiController(TransactionRepository transactionRepository,
                                  KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper) {
        this.transactionRepository = transactionRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        return ResponseEntity.ok(transactionRepository.getDashboardStats());
    }

    @GetMapping("/transactions")
    public ResponseEntity<List<Map<String, Object>>> getRecentTransactions(
            @RequestParam(defaultValue = "20") int limit) {
        return ResponseEntity.ok(transactionRepository.getRecentTransactions(limit));
    }

    @GetMapping("/alerts")
    public ResponseEntity<List<Map<String, Object>>> getRecentAlerts(
            @RequestParam(defaultValue = "20") int limit) {
        return ResponseEntity.ok(transactionRepository.getRecentAlerts(limit));
    }

    @GetMapping("/rejected")
    public ResponseEntity<List<Map<String, Object>>> getRecentRejected(
            @RequestParam(defaultValue = "20") int limit) {
        return ResponseEntity.ok(transactionRepository.getRecentRejected(limit));
    }

    @PostMapping("/clear")
    public ResponseEntity<Map<String, Object>> clearData() {
        Map<String, Object> response = new HashMap<>();
        try {
            transactionRepository.clearAllData();
            response.put("status", "SUCCESS");
            response.put("message", "All transactions, alerts, and rejected events have been cleared from MySQL.");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to clear database data", e);
            response.put("status", "ERROR");
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    @PostMapping("/send")
    public ResponseEntity<Map<String, Object>> sendTransaction(@RequestBody(required = false) Map<String, Object> request) {
        Map<String, Object> response = new HashMap<>();
        try {
            String scenario = request != null && request.containsKey("scenario")
                    ? String.valueOf(request.get("scenario"))
                    : "NORMAL";

            String accountId = request != null && request.containsKey("accountId") && !String.valueOf(request.get("accountId")).isBlank()
                    ? String.valueOf(request.get("accountId"))
                    : "ACC_" + (100 + new Random().nextInt(50));

            String merchant = request != null && request.containsKey("merchant") && !String.valueOf(request.get("merchant")).isBlank()
                    ? String.valueOf(request.get("merchant"))
                    : "Retail Merchant " + (new Random().nextInt(10) + 1);

            double amount = 1500.00;
            if (request != null && request.containsKey("amount")) {
                amount = Double.parseDouble(String.valueOf(request.get("amount")));
            }

            switch (scenario.toUpperCase()) {
                case "HIGH_VALUE" -> {
                    if (amount <= 50000.00) amount = 75000.00;
                    String txId = "TX_HV_" + txSequence.incrementAndGet();
                    publishEvent(txId, accountId, amount, merchant);
                    response.put("message", "High-Value Transaction (> 50,000 INR) dispatched to Kafka");
                    response.put("transactionId", txId);
                }
                case "RAPID_SPENDING" -> {
                    // Send 3 distinct transactions in rapid succession for the same account
                    String rapidAccount = "ACC_RAPID_" + (new Random().nextInt(1000));
                    List<String> ids = new ArrayList<>();
                    for (int i = 1; i <= 3; i++) {
                        String txId = "TX_RS_" + txSequence.incrementAndGet();
                        ids.add(txId);
                        publishEvent(txId, rapidAccount, 250.00 * i, "Coffee Mart");
                    }
                    response.put("message", "Rapid-Spending Burst (3 tx in 60s) dispatched to Kafka");
                    response.put("transactionIds", ids);
                    response.put("accountId", rapidAccount);
                }
                case "DUPLICATE" -> {
                    // Send identical duplicate
                    String dupTxId = "TX_DUP_" + txSequence.incrementAndGet();
                    publishEvent(dupTxId, accountId, amount, merchant);
                    // Immediately re-publish same event
                    publishEvent(dupTxId, accountId, amount, merchant);
                    response.put("message", "Duplicate Transactions (same txId & payload) dispatched to Kafka");
                    response.put("transactionId", dupTxId);
                }
                case "MALFORMED" -> {
                    String malformed = "{ \"transactionId\": \"BAD_JSON\", \"amount\": ";
                    kafkaTemplate.send(topic, "BAD_KEY", malformed);
                    response.put("message", "Malformed JSON payload dispatched to Kafka (testing poison pill resilience)");
                    response.put("payload", malformed);
                }
                default -> { // NORMAL
                    String txId = "TX_NORM_" + txSequence.incrementAndGet();
                    publishEvent(txId, accountId, amount, merchant);
                    response.put("message", "Normal Transaction dispatched to Kafka");
                    response.put("transactionId", txId);
                }
            }

            response.put("status", "SUCCESS");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("Failed to publish transaction via API", e);
            response.put("status", "ERROR");
            response.put("error", e.getMessage());
            return ResponseEntity.internalServerError().body(response);
        }
    }

    private void publishEvent(String txId, String accountId, double amount, String merchant) throws Exception {
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("transactionId", txId);
        event.put("accountId", accountId);
        event.put("amount", amount);
        event.put("currency", "INR");
        event.put("timestamp", java.time.Instant.now().truncatedTo(java.time.temporal.ChronoUnit.SECONDS).toString());
        event.put("merchant", merchant);

        String payload = objectMapper.writeValueAsString(event);
        kafkaTemplate.send(topic, txId, payload);
        logger.info("[WEB API] Published test event txId='{}', account='{}', amount={} INR to topic '{}'",
                txId, accountId, amount, topic);
    }
}
