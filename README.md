# FinStream — Real-Time Financial Transaction Monitoring System

FinStream is an educational, production-style, real-time financial transaction monitoring pipeline. It is engineered to demonstrate event streaming with Apache Kafka, robust stream consumption with Java 17 and Spring Boot 3.3.4, persistent idempotency with MySQL InnoDB, strict boundary evaluation for monitoring rules, and failure-safe data processing.

---

## 1. System Architecture & Message Flow

```mermaid
flowchart TD
    subgraph Producer ["Python Event Producer"]
        TG[Synthetic Transaction Generator] -->|JSON Event| KP[Kafka Producer Client]
    end

    subgraph KafkaBroker ["Apache Kafka (KRaft Mode :9092)"]
        KP -->|Topic: 'transactions' <br/> acks=all, Key: transactionId| KT[Partition 0]
    end

    subgraph Consumer ["Java 17 / Spring Boot Consumer"]
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

    TXWRITE -->|Write Succeeded| ACK[commit Kafka Offset]
```

---

## 2. Directory Structure

```text
finstream/
├── .env.example                       # Template for local environment variables
├── .gitignore                         # Excludes secrets (.env), venv, target/, logs
├── README.md                          # Comprehensive architectural and runbook documentation
├── database/
│   └── schema.sql                     # DDL for MySQL (transactions, alerts, rejected events)
├── sql/
│   └── analytics.sql                  # Analytical SQL queries for monitoring and auditing
├── scripts/
│   ├── start-kafka.ps1                # PowerShell helper to start KRaft standalone broker
│   └── check-system.ps1               # Environment sanity check script
├── producer/                          # Python event generation and ingestion
│   ├── .venv/                         # Isolated Python virtual environment
│   ├── requirements.txt               # Dependencies (kafka-python, pytest, python-dotenv)
│   ├── transaction_generator.py       # Deterministic & synthetic transaction generation
│   ├── producer.py                    # Kafka producer client with retries and acks=all
│   ├── e2e_acceptance_scenarios.py    # Acceptance suite (Phase 4/5 historical)
│   ├── e2e_phase6_acceptance.py       # Final Phase 6 live end-to-end acceptance suite
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
        │   │   ├── model/             # Models: TransactionEvent, ProcessingResult, etc.
        │   │   ├── repository/TransactionRepository.java
        │   │   ├── service/           # TransactionProcessingService, TransactionValidator
        │   │   └── exception/         # ValidationException, IdCollisionException
        │   └── resources/
        │       └── application.properties
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
  "timestamp": "2026-10-15T12:30:00Z",
  "merchant": "Electronics Megastore"
}
```

### Field Specifications

| Field | Type | Rules |
| :--- | :--- | :--- |
| `transactionId` | String | Non-empty unique identifier; used as the Kafka message key |
| `accountId` | String | Non-empty bank account identifier |
| `amount` | Decimal | Positive number (`> 0`), 2-decimal precision |
| `currency` | String | ISO-4217 3-letter currency code (`INR`) |
| `timestamp` | String | ISO-8601 UTC timestamp ending in `Z` |
| `merchant` | String | Non-empty merchant identifier |

---

## 4. Monitoring Rules & Exact Boundaries

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
- **Poison Pill Handling:** Malformed JSON strings and schema validation errors are caught, logged, written to `rejected_events` (if DB is up), and **immediately acknowledged** so corrupt messages do not cause endless poison-pill loops.

---

## 7. Setup & Reproducible Execution Runbook

### Prerequisites
- **Operating System:** Windows 10/11 with PowerShell
- **Java:** Eclipse Adoptium Temurin JDK 17.0.17 (`java -version`)
- **Maven:** Apache Maven 3.9.16 (`mvn -version`)
- **Kafka:** Apache Kafka 4.2.2 (KRaft mode standalone at `localhost:9092`)
- **MySQL:** MySQL Server 8.0+ / 26.7 (`localhost:3306`)
- **Python:** Python 3.10+ (`python --version`)

### Step 1: Environment Variables Configuration
Copy `.env.example` to `.env` in the project root and fill in local credentials:
```powershell
cp .env.example .env
```
*(Ensure `.env` remains ignored by git)*

Set environment variables in your PowerShell terminal session:
```powershell
$env:MYSQL_HOST     = "localhost"
$env:MYSQL_PORT     = "3306"
$env:MYSQL_DATABASE = "finstream"
$env:MYSQL_USER     = "finstream_user"
$env:MYSQL_PASSWORD = "finstream_dev_2024"
$env:KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
```

### Step 2: Initialize Database Schema
Apply the DDL script using the MySQL client:
```powershell
Get-Content database/schema.sql -Raw | & "C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u $env:MYSQL_USER -p$env:MYSQL_PASSWORD finstream
```

### Step 3: Run Python Unit Tests
Activate the virtual environment and execute pytest:
```powershell
cd producer
.\.venv\Scripts\Activate.ps1
pytest -v
cd ..
```
*Expected: 17 passed.*

### Step 4: Run Java Unit & Integration Tests
Execute the full Maven test suite:
```powershell
cd consumer
$env:JAVA_HOME  = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:MAVEN_HOME = "C:\tools\maven\apache-maven-3.9.16"
$env:PATH       = "$env:JAVA_HOME\bin;$env:MAVEN_HOME\bin;" + $env:PATH

mvn clean test
cd ..
```
*Expected: 32 tests passed, 0 failures, 0 errors.*

### Step 5: Start the Spring Boot Consumer
Build and launch the consumer service:
```powershell
cd consumer
mvn package -DskipTests
java -jar target\finstream-consumer-1.0.0-SNAPSHOT.jar
```

### Step 6: Execute Phase 6 Live End-to-End Acceptance Suite
In a separate PowerShell terminal, run the acceptance verification:
```powershell
cd producer
.\.venv\Scripts\Activate.ps1
python e2e_phase6_acceptance.py
cd ..
```
*Expected: All 10 acceptance scenarios report [PASSED].*

### Step 7: Run Analytical SQL Queries
Verify monitoring analytics against live MySQL data:
```powershell
Get-Content sql/analytics.sql -Raw | & "C:\Program Files\MySQL\MySQL Server 26.7\bin\mysql.exe" -u $env:MYSQL_USER -p$env:MYSQL_PASSWORD finstream
```

---

## 8. Verification Results

| Test Suite | Total Tests | Passed | Failed | Blocked | Scope & Evidence |
| :--- | :---: | :---: | :---: | :---: | :--- |
| **Python Unit Tests** | 17 | 17 | 0 | 0 | `test_generator.py`, `test_producer.py` (Validation & Serialization) |
| **Java Unit Tests** | 17 | 17 | 0 | 0 | `TransactionValidatorTest` (8), `TransactionProcessingServiceTest` (9) |
| **MySQL Integration Tests** | 4 | 4 | 0 | 0 | `TransactionRepositoryIntegrationTest` (CRUD, Rollback, Index checks) |
| **Rapid-Spending Boundaries** | 7 | 7 | 0 | 0 | `RapidSpendingBoundaryIntegrationTest` (Cases 1–8: Exact 60s, Out-of-Order, Restarts) |
| **Outage & Restart Resilience** | 2 | 2 | 0 | 0 | `OutageAndRestartIntegrationTest` (Zero-Ack Outage Simulation & Live Replay Idempotency) |
| **Live Kafka Consumer Test** | 1 | 1 | 0 | 0 | `TransactionKafkaConsumerIntegrationTest` (End-to-end broker consumption) |
| **Spring Context Test** | 1 | 1 | 0 | 0 | `FinStreamApplicationTests` |
| **Phase 6 E2E Acceptance** | 10 | 10 | 0 | 0 | `e2e_phase6_acceptance.py` (10/10 scenarios verified against live MySQL) |
| **Total Test Suite** | **59** | **59** | **0** | **0** | **100% Pass Rate across all unit, integration, and E2E suites** |

---

## 9. Analytical SQL Queries Overview

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

## 10. Known Architectural Limitations & Boundaries

1. **Dual-System Commit Boundary:** Kafka offset commits and MySQL transactions are separate operations without a distributed transaction manager (such as XA / Two-Phase Commit). If the JVM crashes after MySQL commits but before Kafka acknowledges the offset, the event is redelivered on restart. Our system handles this safely via persistent idempotency checks (`matchesEvent`) and unique database constraints.
2. **Clock Alignment:** The rolling 60-second window calculates elapsed time based on event-time UTC timestamps (`transaction_timestamp`). Accurate monitoring requires upstream transaction generation systems to maintain NTP-synchronized clocks.
3. **Historical Alert Immutability:** When out-of-order transactions arrive, the system evaluates the window for the incoming transaction. It does not retroactively rewrite historical alert records generated in the past.
4. **Educational Scope:** This project is designed as an educational reference prototype for technical interviews. It does not include distributed stream clustering (e.g. Flink, Spark Streaming) or distributed caching layers (Redis).
