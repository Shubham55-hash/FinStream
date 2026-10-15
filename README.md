# FinStream — Real-Time Financial Transaction Monitoring System

FinStream is an event-driven financial transaction monitoring pipeline built for real-time fraud and anomaly detection.

## Architecture

```
+--------------------------------+
|  Python Transaction Generator   |
|     (transaction_generator.py) |
+---------------+----------------+
                |
                v
+--------------------------------+
|     Kafka Event Producer       |
|          (producer.py)         |
+---------------+----------------+
                |
                | JSON Events (Key: transactionId)
                v
+--------------------------------+
|      Apache Kafka Broker       |
|      Topic: 'transactions'     |
+---------------+----------------+
                |
                | Consumer Group: finstream-consumer-group
                v
+--------------------------------+
|    Spring Boot Kafka Consumer  |
|  (TransactionKafkaConsumer.java|
+---------------+----------------+
                |
                v
+--------------------------------+
|  Transaction Processing Service|
|  - Schema & Value Validation   |
|  - High-Value Rule (> 50k INR) |
|  - Process Duplicate Tracking  |
+--------------------------------+
```

---

## Shared Transaction Schema

All events adhere to the following JSON schema:

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

## Prerequisites

- **Java**: JDK 17+ (Eclipse Adoptium Temurin 17 recommended)
- **Apache Maven**: 3.9+
- **Python**: 3.10+
- **Apache Kafka**: 4.x (KRaft mode) or 3.x
- **OS**: Windows, macOS, or Linux

---

## Quickstart & Execution Guide

### 1. Start Kafka Broker (KRaft Mode)

```powershell
# Windows PowerShell
.\scripts\start-kafka.ps1
```

Or manually:
```powershell
$env:KAFKA_HEAP_OPTS = "-Xmx1G -Xms1G"
C:\tools\kafka\kafka_2.13-4.2.2\bin\windows\kafka-server-start.bat C:\tools\kafka\kafka_2.13-4.2.2\config\server-finstream.properties
```

Verify topic exists:
```powershell
C:\tools\kafka\kafka_2.13-4.2.2\bin\windows\kafka-topics.bat --bootstrap-server localhost:9092 --list
```

---

### 2. Run Python Producer

```powershell
cd producer

# 1. Activate Virtual Environment
.\.venv\Scripts\Activate.ps1

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run unit tests
pytest -v

# 4. Publish normal transactions
python producer.py --count 10 --interval 0.5 --scenario normal

# 5. Publish test scenarios
python producer.py --count 5 --scenario high-value
python producer.py --count 4 --scenario duplicate
python producer.py --count 15 --scenario mixed
```

---

### 3. Run Spring Boot Consumer

```powershell
cd consumer

# 1. Build and run all tests
mvn clean test

# 2. Run the application
mvn spring-boot:run
```

---

## Offset Management & Error Handling

- **AckMode**: `MANUAL_IMMEDIATE` — offsets are only acknowledged (`acknowledgment.acknowledge()`) after validation and business processing logic finish.
- **Malformed JSON**: Caught cleanly by the service, logged as `REJECTED_MALFORMED_JSON`, and offset committed to prevent poison pill crash loops.
- **Validation Failures**: Invalid business records (e.g. negative amount, blank account) are safely rejected and logged with `REJECTED_VALIDATION_ERROR`.
- **Monitoring Alerts**: Transactions exceeding 50,000 INR trigger `[MONITORING ALERT] HIGH_VALUE`.

---

## Known Limitations & Planned Future Phases

- **Duplicate Detection**: Currently in-memory (`ConcurrentHashMap`) for the JVM process lifetime. Persistent database-backed idempotency will be added in Phase 4.
- **Persistence**: Relational MySQL storage for transactions and alerts will be implemented in subsequent phases.
