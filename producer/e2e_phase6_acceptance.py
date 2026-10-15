"""
FinStream Phase 6 Final End-to-End Acceptance Test Suite.

Executes live data pipeline:
Python Producer -> Apache Kafka -> Spring Boot Consumer -> MySQL Database.

Asserts exact database rows, alerts, deduplication, boundaries, and collision tracking.
Exits with code 0 on success, or code 1 on failure.
"""

import json
import subprocess
import sys
import time
import uuid
from decimal import Decimal
from confluent_kafka import Producer


def query_mysql(sql: str) -> str:
    """Execute SQL query using mysql CLI and return stdout."""
    mysql_user = os.getenv("MYSQL_USER", "finstream_user")
    mysql_pwd = os.getenv("MYSQL_PASSWORD", "finstream_dev_2024")
    cmd = [
        r"C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe",
        "-u", mysql_user,
        f"-p{mysql_pwd}",
        "finstream",
        "-N", "-s",
        "-e", sql,
    ]
    res = subprocess.run(cmd, capture_output=True, text=True)
    if res.returncode != 0:
        raise RuntimeError(f"MySQL query failed: {res.stderr.strip()}")
    return res.stdout.strip()


def wait_for_condition(check_fn, timeout_sec: float = 12.0, poll_sec: float = 0.5):
    """Poll condition until true or timeout."""
    start = time.time()
    while time.time() - start < timeout_sec:
        res = check_fn()
        if res:
            return res
        time.sleep(poll_sec)
    return False


def main():
    run_id = uuid.uuid4().hex[:6].upper()
    prefix = f"TX_P6_{run_id}"

    print("================================================================================")
    print(f"FINSTREAM PHASE 6 END-TO-END ACCEPTANCE SUITE (Run ID: {run_id})")
    print("================================================================================")

    producer = Producer({
        "bootstrap.servers": "localhost:9092",
        "client.id": f"finstream-p6-{run_id}",
        "acks": "all",
    })

    topic = "transactions"
    results = {}

    def publish(key: str, value_bytes: bytes):
        producer.produce(topic, key=key.encode("utf-8"), value=value_bytes)
        producer.poll(0)

    # -------------------------------------------------------------------------
    # Scenario 1: Normal Valid Transaction (1,500 INR)
    # -------------------------------------------------------------------------
    tx1_id = f"{prefix}_01_NORM"
    acc1 = f"ACC_P6_{run_id}_NORM"
    tx1 = {
        "transactionId": tx1_id,
        "accountId": acc1,
        "amount": 1500.00,
        "currency": "INR",
        "timestamp": "2026-10-15T16:00:00Z",
        "merchant": "Demo Store",
    }
    publish(tx1_id, json.dumps(tx1).encode("utf-8"))

    # -------------------------------------------------------------------------
    # Scenario 2: High-Value Transaction (> 50,000 INR)
    # -------------------------------------------------------------------------
    tx2_id = f"{prefix}_02_HIGH"
    acc2 = f"ACC_P6_{run_id}_HIGH"
    tx2 = {
        "transactionId": tx2_id,
        "accountId": acc2,
        "amount": 75000.00,
        "currency": "INR",
        "timestamp": "2026-10-15T16:00:05Z",
        "merchant": "Croma Digital",
    }
    publish(tx2_id, json.dumps(tx2).encode("utf-8"))

    # -------------------------------------------------------------------------
    # Scenario 3: Exact 50,000 INR Boundary (Must NOT trigger high-value)
    # -------------------------------------------------------------------------
    tx3_id = f"{prefix}_03_BOUND50K"
    acc3 = f"ACC_P6_{run_id}_BOUND50K"
    tx3 = {
        "transactionId": tx3_id,
        "accountId": acc3,
        "amount": 50000.00,
        "currency": "INR",
        "timestamp": "2026-10-15T16:00:10Z",
        "merchant": "Reliance Digital",
    }
    publish(tx3_id, json.dumps(tx3).encode("utf-8"))

    # -------------------------------------------------------------------------
    # Scenario 4 & 5: Two distinct in 60s (ACCEPTED), then Third in 60s (RAPID_SPENDING)
    # -------------------------------------------------------------------------
    acc_rapid = f"ACC_P6_{run_id}_RAPID"
    tx4a_id = f"{prefix}_04_RAPID_A"
    tx4a = {
        "transactionId": tx4a_id,
        "accountId": acc_rapid,
        "amount": 500.00,
        "currency": "INR",
        "timestamp": "2026-10-15T16:01:00Z",
        "merchant": "Swiggy",
    }
    publish(tx4a_id, json.dumps(tx4a).encode("utf-8"))

    tx4b_id = f"{prefix}_04_RAPID_B"
    tx4b = {
        "transactionId": tx4b_id,
        "accountId": acc_rapid,
        "amount": 1000.00,
        "currency": "INR",
        "timestamp": "2026-10-15T16:01:20Z",
        "merchant": "Zomato",
    }
    publish(tx4b_id, json.dumps(tx4b).encode("utf-8"))

    tx5_id = f"{prefix}_05_RAPID_C"
    tx5 = {
        "transactionId": tx5_id,
        "accountId": acc_rapid,
        "amount": 1500.00,
        "currency": "INR",
        "timestamp": "2026-10-15T16:01:40Z",
        "merchant": "Amazon India",
    }
    publish(tx5_id, json.dumps(tx5).encode("utf-8"))

    # -------------------------------------------------------------------------
    # Scenario 6: Exact 60-Second Boundary (t0, t0+30s, t0+60s -> RAPID_SPENDING)
    # -------------------------------------------------------------------------
    acc_exact = f"ACC_P6_{run_id}_EXACT"
    tx6a_id = f"{prefix}_06_EXACT_A"
    tx6a = {"transactionId": tx6a_id, "accountId": acc_exact, "amount": 100.00, "currency": "INR", "timestamp": "2026-10-15T16:05:00Z", "merchant": "Shop"}
    publish(tx6a_id, json.dumps(tx6a).encode("utf-8"))

    tx6b_id = f"{prefix}_06_EXACT_B"
    tx6b = {"transactionId": tx6b_id, "accountId": acc_exact, "amount": 100.00, "currency": "INR", "timestamp": "2026-10-15T16:05:30Z", "merchant": "Shop"}
    publish(tx6b_id, json.dumps(tx6b).encode("utf-8"))

    tx6c_id = f"{prefix}_06_EXACT_C"
    tx6c = {"transactionId": tx6c_id, "accountId": acc_exact, "amount": 100.00, "currency": "INR", "timestamp": "2026-10-15T16:06:00Z", "merchant": "Shop"}
    publish(tx6c_id, json.dumps(tx6c).encode("utf-8"))

    # -------------------------------------------------------------------------
    # Scenario 7: Identical Duplicate Delivery of tx1
    # -------------------------------------------------------------------------
    publish(tx1_id, json.dumps(tx1).encode("utf-8"))

    # -------------------------------------------------------------------------
    # Scenario 8: Conflicting Transaction ID Collision
    # -------------------------------------------------------------------------
    tx8_collision = {
        "transactionId": tx1_id,  # same txId as tx1
        "accountId": f"ACC_CONFLICT_{run_id}",
        "amount": 99999.00,
        "currency": "INR",
        "timestamp": "2026-10-15T16:10:00Z",
        "merchant": "Rogue Attacker",
    }
    publish(tx1_id, json.dumps(tx8_collision).encode("utf-8"))

    # -------------------------------------------------------------------------
    # Scenario 9: Invalid Transaction (Negative Amount)
    # -------------------------------------------------------------------------
    tx9_id = f"{prefix}_09_INVALID"
    tx9 = {
        "transactionId": tx9_id,
        "accountId": f"ACC_BAD_{run_id}",
        "amount": -500.00,
        "currency": "INR",
        "timestamp": "2026-10-15T16:15:00Z",
        "merchant": "Bad Store",
    }
    publish(tx9_id, json.dumps(tx9).encode("utf-8"))

    # -------------------------------------------------------------------------
    # Scenario 10: Malformed JSON followed by valid transaction
    # -------------------------------------------------------------------------
    publish("POISON_PILL_RAW", b"{ invalid_unquoted_json_string_payload ")

    tx10_valid_id = f"{prefix}_10_VALID_AFTER_POISON"
    tx10_valid = {
        "transactionId": tx10_valid_id,
        "accountId": f"ACC_RESILIENT_{run_id}",
        "amount": 2200.00,
        "currency": "INR",
        "timestamp": "2026-10-15T16:20:00Z",
        "merchant": "Resilient Store",
    }
    publish(tx10_valid_id, json.dumps(tx10_valid).encode("utf-8"))

    # Flush all events to Kafka
    producer.flush(timeout=10.0)
    print("All test events published to Kafka broker. Awaiting consumer processing and MySQL persistence...")

    # =========================================================================
    # ASSERTIONS AGAINST MYSQL
    # =========================================================================

    # 1. Normal Transaction
    s1_ok = wait_for_condition(lambda: query_mysql(f"SELECT status FROM transactions WHERE transaction_id='{tx1_id}'") == "ACCEPTED")
    results["1. Normal Transaction (ACCEPTED)"] = s1_ok

    # 2. High-Value Transaction (> 50k INR)
    s2_status_ok = wait_for_condition(lambda: query_mysql(f"SELECT status FROM transactions WHERE transaction_id='{tx2_id}'") == "HIGH_VALUE")
    s2_alert_ok = query_mysql(f"SELECT COUNT(*) FROM transaction_alerts WHERE transaction_id='{tx2_id}' AND alert_type='HIGH_VALUE'") == "1"
    results["2. High-Value Transaction (> 50k INR -> FLAGGED_HIGH_VALUE)"] = s2_status_ok and s2_alert_ok

    # 3. Exactly 50,000 INR Boundary (ACCEPTED, no alert)
    s3_status_ok = wait_for_condition(lambda: query_mysql(f"SELECT status FROM transactions WHERE transaction_id='{tx3_id}'") == "ACCEPTED")
    s3_no_alert = query_mysql(f"SELECT COUNT(*) FROM transaction_alerts WHERE transaction_id='{tx3_id}'") == "0"
    results["3. Exactly 50,000 INR Boundary (ACCEPTED, zero alerts)"] = s3_status_ok and s3_no_alert

    # 4 & 5. Rapid Spending Window: Tx 4a and 4b ACCEPTED, Tx 5 FLAGGED_RAPID_SPENDING
    s4a_ok = wait_for_condition(lambda: query_mysql(f"SELECT status FROM transactions WHERE transaction_id='{tx4a_id}'") == "ACCEPTED")
    s4b_ok = wait_for_condition(lambda: query_mysql(f"SELECT status FROM transactions WHERE transaction_id='{tx4b_id}'") == "ACCEPTED")
    s5_ok = wait_for_condition(lambda: query_mysql(f"SELECT status FROM transactions WHERE transaction_id='{tx5_id}'") == "RAPID_SPENDING")
    s5_alert = query_mysql(f"SELECT COUNT(*) FROM transaction_alerts WHERE transaction_id='{tx5_id}' AND alert_type='RAPID_SPENDING'") == "1"
    results["4 & 5. Rapid Spending (2 inside window ACCEPTED, 3rd triggers RAPID_SPENDING)"] = s4a_ok and s4b_ok and s5_ok and s5_alert

    # 6. Exact 60-Second Boundary (Inclusive: triggers RAPID_SPENDING)
    s6_ok = wait_for_condition(lambda: query_mysql(f"SELECT status FROM transactions WHERE transaction_id='{tx6c_id}'") == "RAPID_SPENDING")
    results["6. Exact 60-Second Boundary (Inclusive: triggers RAPID_SPENDING)"] = bool(s6_ok)

    # 7. Duplicate Delivery (Zero extra rows created in MySQL)
    tx1_count = query_mysql(f"SELECT COUNT(*) FROM transactions WHERE transaction_id='{tx1_id}'")
    results["7. Duplicate Delivery Idempotency (Exactly 1 row in transactions)"] = (tx1_count == "1")

    # 8. Conflicting Transaction ID Collision
    s8_collision_alert = wait_for_condition(lambda: query_mysql(f"SELECT COUNT(*) FROM transaction_alerts WHERE transaction_id='{tx1_id}' AND alert_type='ID_COLLISION'") == "1")
    s8_collision_rejected = query_mysql(f"SELECT COUNT(*) FROM rejected_events WHERE transaction_id='{tx1_id}' AND rejection_type='ID_COLLISION'") == "1"
    # Ensure original row was NOT modified
    s8_orig_account = query_mysql(f"SELECT account_id FROM transactions WHERE transaction_id='{tx1_id}'") == acc1
    results["8. ID Collision Detection (Recorded in alerts/rejected, original row unmodified)"] = bool(s8_collision_alert) and s8_collision_rejected and s8_orig_account

    # 9. Invalid Transaction (Recorded in rejected_events, NOT in transactions)
    s9_rejected = wait_for_condition(lambda: query_mysql(f"SELECT COUNT(*) FROM rejected_events WHERE transaction_id='{tx9_id}' AND rejection_type='VALIDATION_ERROR'") == "1")
    s9_not_in_tx = query_mysql(f"SELECT COUNT(*) FROM transactions WHERE transaction_id='{tx9_id}'") == "0"
    results["9. Invalid Transaction Handling (VALIDATION_ERROR in rejected_events, not in transactions)"] = bool(s9_rejected) and s9_not_in_tx

    # 10. Malformed JSON followed by valid transaction
    s10_malformed = wait_for_condition(lambda: int(query_mysql("SELECT COUNT(*) FROM rejected_events WHERE rejection_type='MALFORMED_JSON'")) >= 1)
    s10_valid_after = wait_for_condition(lambda: query_mysql(f"SELECT status FROM transactions WHERE transaction_id='{tx10_valid_id}'") == "ACCEPTED")
    results["10. Poison Pill Resilience (Malformed JSON rejected, subsequent valid event accepted)"] = bool(s10_malformed) and bool(s10_valid_after)

    # Print summary
    print("\n================================================================================")
    print("ACCEPTANCE SCENARIO RESULTS:")
    print("================================================================================")
    all_passed = True
    for name, passed in results.items():
        status_label = "[PASS]" if passed else "[FAIL]"
        print(f"  {status_label} {name}")
        if not passed:
            all_passed = False

    print("================================================================================")
    if all_passed:
        print(f"ALL SCENARIOS PASSED SUCCESSFULLY! (Run ID: {run_id})")
        sys.exit(0)
    else:
        print(f"ONE OR MORE SCENARIOS FAILED! (Run ID: {run_id})")
        sys.exit(1)


if __name__ == "__main__":
    main()
