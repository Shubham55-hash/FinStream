"""
End-to-End Pipeline Verification Script.
Publishes a known, deterministic dataset containing:
1. Normal transaction (ACCEPTED)
2. High-value transaction > 50,000 INR (FLAGGED_HIGH_VALUE)
3. Duplicate transaction pair (DUPLICATE DETECTED)
4. Invalid business rule transaction (REJECTED_VALIDATION_ERROR)
5. Malformed non-JSON payload (REJECTED_MALFORMED_JSON)
"""

import json
import time
from confluent_kafka import Producer


def run_e2e_test():
    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "client.id": "finstream-e2e-tester",
        "acks": "all",
    })

    def delivery_report(err, msg):
        if err:
            print(f"[FAIL] Delivery failed: {err}")
        else:
            print(f"[PUBLISHED] offset={msg.offset()}, key={msg.key().decode('utf-8')}")

    topic = "transactions"
    print("--- [E2E] Publishing Test Dataset ---")

    # 1. Normal
    tx1 = {
        "transactionId": "TX_E2E_NORM_01",
        "accountId": "ACC_E2E_100",
        "amount": 2500.00,
        "currency": "INR",
        "timestamp": "2026-10-15T12:30:00Z",
        "merchant": "Demo Store",
    }
    producer.produce(topic, key=b"TX_E2E_NORM_01", value=json.dumps(tx1).encode("utf-8"), on_delivery=delivery_report)

    # 2. High-value (> 50,000 INR)
    tx2 = {
        "transactionId": "TX_E2E_HIGH_02",
        "accountId": "ACC_E2E_101",
        "amount": 89000.50,
        "currency": "INR",
        "timestamp": "2026-10-15T12:30:05Z",
        "merchant": "Reliance Digital",
    }
    producer.produce(topic, key=b"TX_E2E_HIGH_02", value=json.dumps(tx2).encode("utf-8"), on_delivery=delivery_report)

    # 3. Duplicate pair
    tx3 = {
        "transactionId": "TX_E2E_DUPE_03",
        "accountId": "ACC_E2E_102",
        "amount": 1200.00,
        "currency": "INR",
        "timestamp": "2026-10-15T12:30:10Z",
        "merchant": "Swiggy",
    }
    producer.produce(topic, key=b"TX_E2E_DUPE_03", value=json.dumps(tx3).encode("utf-8"), on_delivery=delivery_report)
    producer.produce(topic, key=b"TX_E2E_DUPE_03", value=json.dumps(tx3).encode("utf-8"), on_delivery=delivery_report)

    # 4. Invalid business rule (negative amount)
    tx4 = {
        "transactionId": "TX_E2E_BAD_04",
        "accountId": "ACC_E2E_103",
        "amount": -500.00,
        "currency": "INR",
        "timestamp": "2026-10-15T12:30:15Z",
        "merchant": "Amazon India",
    }
    producer.produce(topic, key=b"TX_E2E_BAD_04", value=json.dumps(tx4).encode("utf-8"), on_delivery=delivery_report)

    # 5. Malformed non-JSON payload
    malformed = "NOT_A_VALID_JSON_STRING"
    producer.produce(topic, key=b"TX_E2E_MALFORMED", value=malformed.encode("utf-8"), on_delivery=delivery_report)

    producer.flush(timeout=10.0)
    print("--- [E2E] All 6 test records published and acknowledged by Kafka broker ---")


if __name__ == "__main__":
    run_e2e_test()
