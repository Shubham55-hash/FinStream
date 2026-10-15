# FinStream — Real-Time Financial Transaction Monitoring System

FinStream is an educational, production-style, real-time financial transaction monitoring pipeline and fraud detection system. It is engineered to demonstrate event streaming with Apache Kafka, robust stream processing with Java 17 and Spring Boot 3.3.4, persistent idempotency with MySQL InnoDB, strict boundary evaluation for monitoring rules, and a clean, real-time web monitoring console.

---

## 1. System Architecture & Message Flow

```mermaid
flowchart TD
    subgraph Producer ["Python Event Ingestion Engine"]
        TG[Synthetic Transaction Generator] -->|JSON Event| KP[Kafka Producer Client]
    end

    subgraph KafkaBroker ["Apache Kafka (KRaft Mode :9092)"]
        KP -->|Topic: 'transactions' <br/> acks=all, Key: transactionId| KT[Partition 0]
    end

    subgraph Consumer ["Java 17 / Spring Boot Stream Consumer (:8080)"]
        KT -->|Manual Immediate Acknowledgment| KC[TransactionKafkaConsumer]
        KC --> PS[TransactionProcessingService]
        PS -->|1. Validate Schema & Contract| VAL{Valid?}
        VAL -->|No: Malformed or Invalid| REJ[Save to rejected_events <br/> Acknowledge Offset]
        VAL -->|Yes| IDEM{Check MySQL for tx_id}
        IDEM -->|Identical Payload| DUP[Acknowledge as DUPLICATE_DELIVERY <br/> Do NOT insert or refire alerts]
        IDEM -->|Different Payload| COL[Acknowledge as ID_COLLISION <br/> Record alert & rejected_events]
        IDEM -->|New Transaction| RULES[Evaluate Monitoring Rules]
        RULES -->|Amount > 50,000 INR| HVA[Trigger HIGH_VALUE Alert]
        RULES -->|>= 3 Tx in [t-60s, t]| RSA[Trigger RAPID_SPENDING Alert]
        HVA --> TXWRITE[Atomic / Idempotent MySQL Persistence]
        RSA --> TXWRITE
    end

    subgraph MySQLDB ["MySQL Relational Store (:3306)"]
        TXWRITE -->|Unique tx_id| TBL_TX[(transactions)]
        TXWRITE -->|uk_alerts_tx_type| TBL_AL[(transaction_alerts)]
        REJ --> TBL_RJ[(rejected_events)]
    end

    subgraph WebDashboard ["Real-Time Web Console (Vanilla JS / CSS)"]
        REST[REST API Controller] -->|/api/stats, /api/transactions, /api/alerts, /api/rejected| UI[Live Dark Dashboard]
        UI -->|Manual Trigger /api/send| KP
        UI -->|Clear Data /api/clear| MySQLDB
    end

    TXWRITE -->|Write Succeeded| ACK[Commit Kafka Offset]
```

---

## 2. Directory Structure

```text
finstream/
├── .env.example                       # Template for local environment variables (no secrets)
├── .gitignore                         # Excludes secrets (.env), venv, target/, logs
├── README.md                          # Comprehensive architectural and runbook documentation
├── database/
│   └── schema.sql                     # DDL for MySQL (transactions, alerts, rejected events)
├── sql/
│   └── analytics.sql                  # Analytical SQL queries for monitoring and auditing
├── scripts/
│   └── start-kafka.ps1                # PowerShell helper to start KRaft standalone broker
├── producer/                          # Python event generation and ingestion
│   ├── .venv/                         # Isolated Python virtual environment (ignored)
│   ├── requirements.txt               # Dependencies (confluent-kafka, pytest, python-dotenv)
│   ├── transaction_generator.py       # Deterministic & synthetic transaction generation
│   ├── producer.py                    # Kafka producer client with retries and acks=all
│   ├── e2e_acceptance_scenarios.py    # Acceptance suite (Phase 4/5 historical)
│   ├── e2e_phase6_acceptance.py       # Phase 6 live end-to-end acceptance suite
│   └── tests/
│       ├── test_generator.py          # Generator unit tests
│       └── test_producer.py           # Producer validation unit tests
└── consumer/                          # Java / Spring Boot stream processing service
    ├── pom.xml                        # Maven build file (Spring Boot 3.3.4, Java 17)
    └── src/
        ├── main/
        │   ├── java/com/finstream/
        │   │   ├── FinStreamApplication.java
        │   │   ├── config/KafkaConsumerConfig.java
        │   │   ├── consumer/TransactionKafkaConsumer.java
        │   │   ├── controller/DashboardApiController.java   # Live REST API
        │   │   ├── model/             # Models: TransactionEvent, ProcessingResult, etc.
        │   │   ├── repository/TransactionRepository.java
        │   │   ├── service/           # TransactionProcessingService, TransactionValidator
        │   │   └── exception/         # ValidationException, IdCollisionException
        │   └── resources/
        │       ├── application.yml    # Datasource, Kafka, and Hikari configuration
        │       └── static/            # Clean, dark-mode real-time web dashboard
        │           ├── index.html
        │           ├── css/style.css
        │           └── js/app.js
        └── test/
            └── java/com/finstream/
                ├── FinStreamApplicationTests.java
                ├── consumer/          # Kafka listener & outage/restart integration tests
                ├── repository/        # MySQL JDBC integration tests
                └── service/           # Validator, mock processing, & boundary tests
```

---

## 3. Transaction Contract & Schema

Incoming Kafka messages use UTF-8 JSON. Every message key is set to `transactionId` to ensure strict per-transaction partition ordering.

```json
{
  "transactionId": "TX_P6_DEMO_001",
  "accountId": "ACC_DEMO_101",
  "amount": 75000.00,
  "currency": "INR",
  "timestamp": "2026-10-15T20:15:20Z",
  "merchant": "Cloud Retail Store"
}
```

### Field Specifications

| Field | Type | Rules |
| :--- | :--- | :--- |
| `transactionId` | String | Non-empty unique identifier; used as the Kafka message key |
| `accountId` | String | Non-empty bank account identifier |
| `amount` | Decimal | Positive number (`> 0`), 2-decimal precision |
| `currency` | String | ISO-4217 3-letter currency code (`INR`) |
| `timestamp` | String | ISO-8601 UTC timestamp ending in `Z` (`YYYY-MM-DDTHH:mm:ssZ`) |
| `merchant` | String | Non-empty merchant identifier |

---

## 4. Monitoring Rules & Exact Mathematical Boundaries

### A. High-Value Monitoring Rule
- **Condition:** Amount **strictly greater than** INR 50,000 (`amount > 50000.00`).
- **Boundary Proof:**
  - `50,000.00 INR` $\rightarrow$ Evaluated as `ACCEPTED` (no alert).
  - `50,000.01 INR` $\rightarrow$ Evaluated as `HIGH_VALUE` (alert triggered).
- **Implementation:** Exact precision arithmetic using Java `BigDecimal.compareTo()`.

### B. Rapid-Spending Monitoring Rule
- **Condition:** An account has **at least 3 distinct valid transactions** within a rolling 60-second event-time window.
- **Window Boundaries:** Closed interval $[t - 60\text{s}, t]$ inclusive of both endpoints.
- **Boundary Verification:**
  - $t - 60.000\text{s}$ (exact boundary) $\rightarrow$ **Included** in count.
  - $t - 60.001\text{s}$ (outside boundary) $\rightarrow$ **Excluded** from count.
- **Query Mechanism:**
  ```sql
  SELECT COUNT(DISTINCT transaction_id)
  FROM transactions
  WHERE account_id = ?
    AND transaction_timestamp >= ?
    AND transaction_timestamp <= ?
  ```
- **Rule Independence:** Replayed duplicates and invalid/rejected events do not increment the count. If an event is both high-value and triggers rapid-spending, both alerts are persisted and status is set to `HIGH_VALUE_AND_RAPID_SPENDING`.

---

## 5. Duplicate, Collision, & Idempotency Strategy

| Scenario | Definition | Consumer Behavior | Database Effect |
| :--- | :--- | :--- | :--- |
| **Normal Valid Event** | New `transaction_id`. | Validated, monitored, inserted. | 1 row inserted into `transactions`, alerts logged if rules trigger. |
| **Identical Duplicate** | Existing `transaction_id` with exact matching fields. | Recognized as `DUPLICATE_DELIVERY`. Handled safely. | No new transaction row. Existing alerts are NOT duplicated. |
| **ID Collision** | Existing `transaction_id` with altered data (e.g. changed amount). | Detected as `ID_COLLISION`. Original data defended. | Original transaction preserved untouched. Audit row recorded in `rejected_events` and security alert in `transaction_alerts`. |
| **Alert Idempotency** | Event redelivery or retry after transient failure. | Table constraint `CONSTRAINT uk_alerts_tx_type UNIQUE (transaction_id, alert_type)`. | `ON DUPLICATE KEY UPDATE` guarantees no duplicate alert rows on replay. |

---

## 6. Failure Handling, Acknowledgment, & Outage Recovery

- **Acknowledgment Strategy:** Spring Kafka `AckMode.MANUAL_IMMEDIATE` with `enable-auto-commit: false`.
- **Commit Ordering:** The consumer executes `acknowledgment.acknowledge()` **strictly after** the database write succeeds.
- **Database Outage Behavior:**
  1. If MySQL is unreachable, the repository throws `DataAccessException`.
  2. Spring Kafka's `DefaultErrorHandler` intercepts the failure and initiates bounded retries (3 attempts with 500ms backoff).
  3. Because the exception escapes unhandled, **no acknowledgment is sent to Kafka**.
  4. The unacknowledged message remains at its Kafka offset.
  5. When MySQL connectivity is restored or the consumer restarts, the message is re-read from Kafka and processed safely via MySQL-level idempotency.
- **Poison Pill Handling (Malformed JSON):**
  Malformed JSON strings and schema validation errors are caught, logged, written to the **`rejected_events`** dead-letter table, and **immediately acknowledged**. This prevents the consumer from entering an infinite crash loop, keeping the stream healthy.

---

## 7. Real-Time Web Monitoring Console

FinStream includes an embedded real-time web monitoring console served directly by Spring Boot on port 8080:

👉 **URL:** `http://localhost:8080` (or `http://localhost:8080/index.html`)

### Dashboard Features
1. **Live Metrics Bar:** Displays real-time counts of transactions, processed volume in INR, high-value flags, rapid-spending triggers, security alerts, and dead-letter rejected events.
2. **Interactive Transaction Simulator:**
   - **Custom Event Form:** Input any custom Account ID, Amount, and Merchant to publish transactions directly to Kafka.
   - **One-Click Scenario Buttons:**
     - `Normal (1,500)`: Standard valid event.
     - `High-Value (75,000)`: Event $> 50,000$ INR triggering high-value detection.
     - `Rapid-Burst (3x)`: Dispatches 3 events in rapid succession triggering rapid-spending.
     - `Duplicate Replay`: Dispatches identical duplicate transactions to verify idempotency.
     - `Poison Pill`: Publishes malformed JSON to verify consumer resilience and dead-letter quarantine.
3. **Real-Time Processed Transactions Table:** Live feed auto-refreshing every 2.5 seconds with status badges (`ACCEPTED`, `HIGH_VALUE`, `RAPID_SPENDING`, etc.).
4. **Fraud & Security Alerts Feed:** Dedicated stream of triggered alerts with exact timestamps and violation reasons.
5. **Rejected & Malformed Events Panel:** Real-time dead-letter audit log showing rejected payloads, error reasons, and timestamps.
6. **Protected "Clear Data" Action:** Safe database reset button with confirmation warning to clear MySQL tables for fresh testing runs.

---

## 8. Setup & Reproducible Execution Runbook

### Prerequisites
- **Operating System:** Windows 10/11 with PowerShell (or Linux/macOS bash)
- **Java:** Eclipse Adoptium Temurin JDK 17 (`java -version`)
- **Maven:** Apache Maven 3.9+ (`mvn -version`)
- **Kafka:** Apache Kafka 4.2.2 (KRaft mode standalone at `localhost:9092`)
- **MySQL:** MySQL Server 8.0+ (`localhost:3306`)
- **Python:** Python 3.10+ (`python --version`)

### Step 1: Environment Variables Configuration
Copy `.env.example` to `.env` in the project root and fill in your local credentials:
```powershell
cp .env.example .env
```
*(Ensure `.env` remains ignored by git)*

Set environment variables in your terminal session:
```powershell
$env:MYSQL_HOST              = "localhost"
$env:MYSQL_PORT              = "3306"
$env:MYSQL_DATABASE          = "finstream"
$env:MYSQL_USER              = "finstream_user"
$env:MYSQL_PASSWORD          = "your_password"
$env:KAFKA_BOOTSTRAP_SERVERS  = "localhost:9092"
```

### Step 2: Initialize Database Schema
Apply the DDL script using the MySQL client:
```powershell
Get-Content database/schema.sql -Raw | & mysql -u $env:MYSQL_USER -p$env:MYSQL_PASSWORD finstream
```

### Step 3: Run Python Unit Tests
Activate the virtual environment and execute pytest:
```powershell
cd producer
.\.venv\Scripts\Activate.ps1
pytest -v
cd ..
```
*Result: 17 passed in 0.15s.*

### Step 4: Run Java Unit & Integration Tests
Execute the full Maven test suite:
```powershell
cd consumer
mvn clean test
cd ..
```
*Result: 32 tests passed, 0 failures, 0 errors.*

### Step 5: Start the Spring Boot Consumer & Dashboard
Build and launch the consumer service:
```powershell
cd consumer
mvn clean package -DskipTests
java -jar target\finstream-consumer-1.0.0-SNAPSHOT.jar
```
Once started, access the live dashboard at **`http://localhost:8080`**.

### Step 6: Execute Phase 6 Live End-to-End Acceptance Suite
In a separate terminal, run the acceptance verification:
```powershell
cd producer
.\.venv\Scripts\Activate.ps1
python e2e_phase6_acceptance.py
cd ..
```
*Result: All 10 acceptance scenarios report [PASSED].*

### Step 7: Run Analytical SQL Queries
Verify monitoring analytics against live MySQL data:
```powershell
Get-Content sql/analytics.sql -Raw | & mysql -u $env:MYSQL_USER -p$env:MYSQL_PASSWORD finstream
```

---

## 9. Verification & Test Summary Table

| Test Suite | Total Tests | Passed | Failed | Blocked | Scope & Evidence |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Python Unit Tests** | 17 | **17** | 0 | 0 | `test_generator.py`, `test_producer.py` (Validation & Serialization) |
| **Java Unit Tests** | 17 | **17** | 0 | 0 | `TransactionValidatorTest` (8), `TransactionProcessingServiceTest` (9) |
| **MySQL Integration Tests** | 4 | **4** | 0 | 0 | `TransactionRepositoryIntegrationTest` (CRUD, Rollback, Index checks) |
| **Rapid-Spending Boundaries** | 7 | **7** | 0 | 0 | `RapidSpendingBoundaryIntegrationTest` (Cases 1–8: Exact 60s, Out-of-Order, Restarts) |
| **Outage & Restart Resilience** | 2 | **2** | 0 | 0 | `OutageAndRestartIntegrationTest` (Zero-Ack Outage Simulation & Live Replay Idempotency) |
| **Live Kafka Consumer Test** | 1 | **1** | 0 | 0 | `TransactionKafkaConsumerIntegrationTest` (End-to-end broker consumption) |
| **Spring Context Test** | 1 | **1** | 0 | 0 | `FinStreamApplicationTests` |
| **Phase 6 E2E Acceptance** | 10 | **10** | 0 | 0 | `e2e_phase6_acceptance.py` (All 10 live scenarios evaluated against MySQL) |
| **Total Test Suite** | **59** | **59** | **0** | **0** | **100% Pass Rate across all unit, integration, and E2E suites** |

---

## 10. Analytical SQL Queries Overview

The `sql/analytics.sql` suite provides operational queries aligned with the relational schema:
1. **System Summary:** Total transactions, distinct accounts, accepted transaction volume, and total INR value.
2. **Status Breakdown:** Distribution across `ACCEPTED`, `HIGH_VALUE`, `RAPID_SPENDING`, and `HIGH_VALUE_AND_RAPID_SPENDING`.
3. **High-Value Audit:** Top high-value transactions ordered by amount descending.
4. **Rapid-Spending Audit:** All triggered rapid-spending events with exact timestamps and descriptions.
5. **Top Transacting Accounts:** Highest spenders aggregated by transaction count and total volume.
6. **Hourly Velocity:** Transaction volume and counts bucketed by hour.
7. **Rejection & Malformed Audit:** Audit log of all validation errors, malformed payloads, and security collisions.
8. **Account Investigation Query:** Complete historical transaction trail for any target account.

---

## 11. Known Architectural Limitations & Boundaries

1. **Dual-System Commit Boundary:** Kafka offset commits and MySQL transactions are separate operations without a distributed transaction manager (such as XA / Two-Phase Commit). If the JVM crashes after MySQL commits but before Kafka acknowledges the offset, the event is redelivered on restart. Our system handles this safely via persistent idempotency checks (`matchesEvent`) and unique database constraints.
2. **Clock Alignment:** The rolling 60-second window calculates elapsed time based on event-time UTC timestamps (`transaction_timestamp`). Accurate monitoring requires upstream transaction generation systems to maintain NTP-synchronized clocks.
3. **Historical Alert Immutability:** When out-of-order transactions arrive, the system evaluates the window for the incoming transaction. It does not retroactively rewrite historical alert records generated in the past.
4. **Educational Scope:** This project is designed as an educational reference prototype for technical interviews. It does not include distributed stream clustering (e.g. Flink, Spark Streaming) or distributed caching layers (Redis).
