package com.finstream.consumer;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@TestPropertySource(properties = {
        "spring.kafka.consumer.group-id=test-group-consumer-live",
        "spring.kafka.consumer.auto-offset-reset=earliest"
})
class TransactionKafkaConsumerIntegrationTest {

    @Autowired
    private TransactionKafkaConsumer consumer;

    @Autowired
    private KafkaTemplate<String, String> testKafkaTemplate;

    @Value("${finstream.kafka.topic:transactions}")
    private String topic;

    @Test
    void testLiveKafkaConsumption() throws Exception {
        String testTxId = "TX_LIVE_TEST_" + UUID.randomUUID().toString().substring(0, 8);
        String payload = "{"
                + "\"transactionId\":\"" + testTxId + "\","
                + "\"accountId\":\"ACC_TEST_01\","
                + "\"amount\":5500.00,"
                + "\"currency\":\"INR\","
                + "\"timestamp\":\"2026-10-15T12:30:00Z\","
                + "\"merchant\":\"Integration Store\""
                + "}";

        long initialCount = consumer.getConsumedCount();

        // Send to real Kafka broker
        testKafkaTemplate.send(topic, testTxId, payload).get(5, TimeUnit.SECONDS);

        // Await consumption by Spring Boot consumer listener (up to 15 seconds)
        boolean consumed = false;
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 15000) {
            if (consumer.getConsumedCount() > initialCount) {
                consumed = true;
                break;
            }
            Thread.sleep(200);
        }

        assertTrue(consumed, "Consumer did not receive the test transaction from topic '" + topic + "' within timeout");
    }
}
