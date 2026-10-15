"""
FinStream Phase 4 + Phase 5 End-to-End Acceptance Test Script.
Publishes scenarios 1 through 8 to Kafka topic 'transactions'.
"""

import json
import time
from confluent_kafka import Producer


def run_acceptance_suite():
    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "client.id": "finstream-acceptance-suite",
        "acks": "all",
    })

    def delivery_report(err, msg):
        if err:
            print(f"[FAIL] Delivery failed: {err}")
        else:
            print(f"[PUBLISHED] offset={msg.offset()}, key={msg.key().decode('utf-8')}")

    topic = "transactions"
    print("==================================================================")
    print("FINSTREAM: Publishing Phase 4 + 5 Acceptance Scenarios to Kafka...")
    print("==================================================================")

    # 1. Normal Transaction (ACCEPTED)
    tx1 = {
        "transactionId": "TX_PH45_01_NORM",
        "accountId": "ACC_PH45_NORM",
        "amount": 1500.00,
        "currency": "INR",
        "timestamp": "2026-10-15T15:00:00Z",
        "merchant": "Demo Store",
    }
    producer.produce(topic, key=b"TX_PH45_01_NORM", value=json.dumps(tx1).encode("utf-8"), on_delivery=delivery_report)

    # 2. High-Value Transaction > 50,000 INR (FLAGGED_HIGH_VALUE)
    tx2 = {
        "transactionId": "TX_PH45_02_HIGH",
        "accountId": "ACC_PH45_HIGH",
        "amount": 75000.00,
        "currency": "INR",
        "timestamp": "2026-10-15T15:00:05Z",
        "merchant": "Croma",
    }
    producer.produce(topic, key=b"TX_PH45_02_HIGH", value=json.dumps(tx2).encode("utf-8"), on_delivery=delivery_report)

    # 3. Two different transactions for one account within 60s
    tx3a = {
        "transactionId": "TX_PH45_03_RAPID_A",
        "accountId": "ACC_PH45_RAPID",
        "amount": 1000.00,
        "currency": "INR",
        "timestamp": "2026-10-15T15:01:00Z",
        "merchant": "Swiggy",
    }
    producer.produce(topic, key=b"TX_PH45_03_RAPID_A", value=json.dumps(tx3a).encode("utf-8"), on_delivery=delivery_report)

    tx3b = {
        "transactionId": "TX_PH45_03_RAPID_B",
        "accountId": "ACC_PH45_RAPID",
        "amount": 2000.00,
        "currency": "INR",
        "timestamp": "2026-10-15T15:01:20Z",
        "merchant": "Zomato",
    }
    producer.produce(topic, key=b"TX_PH45_03_RAPID_B", value=json.dumps(tx3b).encode("utf-8"), on_delivery=delivery_report)

    # 4. Third distinct transaction for that account within the 60s window (FLAGGED_RAPID_SPENDING)
    tx4 = {
        "transactionId": "TX_PH45_04_RAPID_C",
        "accountId": "ACC_PH45_RAPID",
        "amount": 3000.00,
        "currency": "INR",
        "timestamp": "2026-10-15T15:01:40Z",
        "merchant": "Amazon India",
    }
    producer.produce(topic, key=b"TX_PH45_04_RAPID_C", value=json.dumps(tx4).encode("utf-8"), on_delivery=delivery_report)

    # 5. Duplicate delivery of existing transaction ID (TX_PH45_01_NORM) with identical data
    producer.produce(topic, key=b"TX_PH45_01_NORM", value=json.dumps(tx1).encode("utf-8"), on_delivery=delivery_report)

    # 6. Same transaction ID with conflicting data (ID COLLISION)
    tx6_collision = {
        "transactionId": "TX_PH45_01_NORM",
        "accountId": "ACC_ROGUE_ATTACKER",
        "amount": 99999.00,
        "currency": "INR",
        "timestamp": "2026-10-15T15:05:00Z",
        "merchant": "Rogue Attacker Merchant",
    }
    producer.produce(topic, key=b"TX_PH45_01_NORM", value=json.dumps(tx6_collision).encode("utf-8"), on_delivery=delivery_report)

    # 7. Invalid transaction (Negative amount -> VALIDATION_ERROR)
    tx7_invalid = {
        "transactionId": "TX_PH45_07_INVALID",
        "accountId": "ACC_INVALID_DATA",
        "amount": -500.00,
        "currency": "INR",
        "timestamp": "2026-10-15T15:06:00Z",
        "merchant": "Invalid Store",
    }
    producer.produce(topic, key=b"TX_PH45_07_INVALID", value=json.dumps(tx7_invalid).encode("utf-8"), on_delivery=delivery_report)

    # 8. Malformed non-JSON payload (MALFORMED_JSON)
    malformed = "{ corrupt-unquoted-json-payload-for-testing-poison-pill "
    producer.produce(topic, key=b"TX_PH45_08_CORRUPT", value=malformed.encode("utf-8"), on_delivery=delivery_report)

    producer.flush(timeout=10.0)
    print("==================================================================")
    print("All 8 acceptance scenarios published to Kafka successfully!")
    print("==================================================================")


if __name__ == "__main__":
    run_acceptance_suite()
