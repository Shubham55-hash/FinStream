# FinStream — Real-Time Financial Transaction Monitoring System

FinStream is an event-driven financial transaction monitoring pipeline engineered to demonstrate real-time fraud detection, persistent idempotency, stream processing, and resilient failure recovery.

---

## 1. System Architecture & Data Flow

```
+---------------------------------------+
|     Python Transaction Generator      |
|      (transaction_generator.py)       |
+-------------------+-------------------+
                    |
                    v
+---------------------------------------+
|         Kafka Event Producer          |
|             (producer.py)             |
+-------------------+-------------------+
                    |
                    | JSON Events (Key: transactionId, acks: all)
                    v
+---------------------------------------+
|          Apache Kafka Broker          |
|         Topic: 'transactions'         |
+-------------------+-------------------+
                    |
                    | Consumer Group: finstream-consumer-group
                    v
+---------------------------------------+
|       Spring Boot Kafka Consumer      |
|    (TransactionKafkaConsumer.java)    |
|   AckMode: MANUAL_IMMEDIATE (post DB) |
+-------------------+-------------------+
                    |
                    v
+---------------------------------------+
|     Transaction Processing Service    |
|   1. Schema & Business Validation     |
|   2. Persistent Idempotency Check     |
|   3. Monitoring Rules:                |
|      - High-Value (> 50,000 INR)      |
|      - Rapid-Spending (>= 3 in 60s)   |
+-------------------+-------------------+
                    |
                    v
+---------------------------------------+
|       MySQL Database (InnoDB)         |
|  - transactions (Unique tx_id)        |
|  - transaction_alerts                 |
|  - rejected_events                    |
+---------------------------------------+
```

---

## 2. Event Schema Contract

All components adhere to the following JSON schema:

```json
{
  "transactionId": "TX1001",
  "accountId": "ACC101",
  "amount": 2500.00,
  "currency": "INR",
  "timestamp": "2026-10-15T12:30:00Z",
  "merchant": "Demo Store"
}
```

### Field Specifications

| Field | Type | Rules |
| :--- | :--- | :--- |
| `transactionId` | String | Non-empty unique identifier, used as the Kafka message key |
| `accountId` | String | Non-empty bank account identifier |
| `amount` | Decimal | Positive number (> 0), 2 decimal precision |
| `currency` | String | 3-letter currency code (`INR`) |
| `timestamp` | String | ISO-8601 UTC timestamp ending in `Z` |
| `merchant` | String | Non-empty merchant identifier |

---

## 3. Database Schema Design

The relational schema (`database/schema.sql`) provides persistent storage, constraint-level duplicate protection, and audit capabilities:

### `transactions`
- `id` BIGINT AUTO_INCREMENT PRIMARY KEY
- `transaction_id` VARCHAR(64) NOT NULL UNIQUE
- `account_id` VARCHAR(64) NOT NULL
- `amount` DECIMAL(19, 2) NOT NULL
- `currency` VARCHAR(3) NOT NULL
- `transaction_timestamp` DATETIME(6) NOT NULL (UTC)
- `merchant` VARCHAR(128) NOT NULL
- `status` VARCHAR(32) NOT NULL (`ACCEPTED`, `HIGH_VALUE`, `RAPID_SPENDING`, `HIGH_VALUE_AND_RAPID_SPENDING`)
- `created_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
- **Indexes**: `(account_id, transaction_timestamp)` for rolling window queries, `(transaction_timestamp)`, `(status)`.

### `transaction_alerts`
- `id` BIGINT AUTO_INCREMENT PRIMARY KEY
- `transaction_id` VARCHAR(64) NOT NULL
- `account_id` VARCHAR(64) NOT NULL
- `alert_type` VARCHAR(32) NOT NULL (`HIGH_VALUE`, `RAPID_SPENDING`, `ID_COLLISION`)
- `description` VARCHAR(255) NOT NULL
- `created_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)

### `rejected_events`
- `id` BIGINT AUTO_INCREMENT PRIMARY KEY
- `transaction_id` VARCHAR(64)
- `payload` TEXT NOT NULL
- `reason` VARCHAR(255) NOT NULL
- `rejection_type` VARCHAR(32) NOT NULL (`VALIDATION_ERROR`, `MALFORMED_JSON`, `ID_COLLISION`)
- `created_at` TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)

---

## 4. Monitoring Rules & Idempotency Strategy

### Persistent Idempotency & Collision Detection
- **Duplicate Delivery:** When an incoming `transaction_id` already exists in the database, the consumer compares all business fields (`accountId`, `amount`, `currency`, `timestamp`, `merchant`). If identical, the event is acknowledged as `DUPLICATE_DELIVERY` without inserting duplicate rows or re-firing alerts.
- **ID Collision:** If an incoming event reuses an existing `transaction_id` with **different payload data**, the system classifies it as an `ID_COLLISION`, records a security alert and audit record in `rejected_events`, and preserves the original row unmodified.
- **Concurrency Safety:** The unique constraint on `transaction_id` provides database-level defense against race conditions across concurrent consumer instances.

### High-Value Transaction Rule
- **Threshold:** Strictly greater than INR 50,000 (`amount > 50000.00`).
- Exact INR 50,000 does **not** trigger the alert.
- Evaluated using exact `BigDecimal` comparisons.

### Rapid-Spending Rule
- **Definition:** An account triggers an alert when it executes **at least 3 distinct transactions within a rolling 60-second window**.
- **Window Boundaries:** Closed interval $[t - 60s, t]$ based on event timestamps.
- **SQL Aggregation:** Query executes `SELECT COUNT(DISTINCT transaction_id) FROM transactions WHERE account_id = ? AND transaction_timestamp >= ? AND transaction_timestamp <= ?`.
- **Dual Rule Alerting:** If a transaction is both high-value and part of a rapid-spending burst, both alerts are recorded in `transaction_alerts` and the transaction status is marked `HIGH_VALUE_AND_RAPID_SPENDING`.

---

## 5. Failure Handling & Offset-Commit Policy

- **Selected Strategy:** `AckMode.MANUAL_IMMEDIATE` with `enable-auto-commit: false`.
- **Commit Boundary:** The consumer invokes `acknowledgment.acknowledge()` **strictly after** the database write completes.
- **Poison Pill Resilience:** Malformed JSON and validation failures are safely logged/recorded and acknowledged so bad payloads do not block the pipeline.
- **Database Outages:** If MySQL experiences a transient outage (`DataAccessException`), the exception is propagated to Spring Kafka's `DefaultErrorHandler` for bounded retries (3 attempts with 500ms backoff). If MySQL remains unavailable, **the offset is NOT acknowledged**. When MySQL recovers or the consumer restarts, uncommitted records are re-read from Kafka and safely deduplicated by MySQL.

---

## 6. Execution & Verification Guide

### Prerequisites
- JDK 17 (Eclipse Adoptium Temurin 17)
- Apache Maven 3.9+
- Python 3.10+
- Apache Kafka 4.2.2 (KRaft mode)
- MySQL Server 8.0+

### Step 1: Start Kafka Broker
```powershell
.\scripts\start-kafka.ps1
```

### Step 2: Initialize Database Schema
```powershell
Get-Content database/schema.sql -Raw | & "C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u finstream_user -pfinstream_dev_2024 finstream
```

### Step 3: Run Producer Unit Tests
```powershell
cd producer
.\.venv\Scripts\Activate.ps1
pytest -v
```

### Step 4: Run Spring Boot Unit & Integration Tests
```powershell
cd consumer
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:MAVEN_HOME = "C:\tools\maven\apache-maven-3.9.16"
$env:PATH = "$env:JAVA_HOME\bin;$env:MAVEN_HOME\bin;" + $env:PATH
mvn clean test
```

### Step 5: Start Spring Boot Consumer
```powershell
cd consumer
mvn package -DskipTests
java -jar target\finstream-consumer-1.0.0-SNAPSHOT.jar
```

### Step 6: Run End-to-End Acceptance Scenarios
```powershell
cd producer
python e2e_acceptance_scenarios.py
```

### Step 7: Run Analytics Queries
```powershell
Get-Content sql/analytics.sql -Raw | & "C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u finstream_user -pfinstream_dev_2024 finstream
```

---

## 7. Known Limitations & Architectural Notes

1. **Dual System Boundary:** Kafka offset commits and MySQL transactions are distinct operations without a two-phase distributed transaction manager (XA). In the rare event of a consumer crash between the database insert and offset commit, the message will be redelivered upon restart. Our database unique constraint and `matchesEvent` logic make repeated delivery idempotent.
2. **Clock Skew:** Rolling window queries rely on the event timestamps in UTC. Upstream systems producing events must have synchronized NTP clocks.
