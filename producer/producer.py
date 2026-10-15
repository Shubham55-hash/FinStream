"""
FinStream - Kafka Financial Transaction Producer.

Publishes synthetic transaction events to Kafka topic 'transactions'.
Features:
- Connects to Kafka broker via configurable bootstrap servers
- Publishes JSON events using transactionId as Kafka message key
- Delivery callbacks report success/failure
- Graceful shutdown with buffer flush
- Scenarios: normal, high-value, duplicate, mixed
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path
from typing import Any, Callable, Dict, Optional

from confluent_kafka import KafkaError, Producer
from dotenv import load_dotenv

from transaction_generator import TransactionGenerator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Producer] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("finstream.producer")


class FinStreamProducer:
    """Publishes transaction events to Apache Kafka with delivery callbacks."""

    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "transactions",
        producer_instance: Optional[Producer] = None,
    ) -> None:
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.success_count = 0
        self.failure_count = 0
        self.running = True

        if producer_instance is not None:
            self._producer = producer_instance
        else:
            config = {
                "bootstrap.servers": self.bootstrap_servers,
                "client.id": "finstream-producer",
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "retry.backoff.ms": 200,
                "linger.ms": 10,
            }
            logger.info("Connecting Kafka Producer to bootstrap servers: %s", self.bootstrap_servers)
            try:
                self._producer = Producer(config)
            except Exception as e:
                logger.error("Failed to initialize Kafka producer: %s", e)
                raise

    def _delivery_report(self, err: Optional[KafkaError], msg: Any) -> None:
        """Callback invoked by confluent-kafka when broker acknowledges message."""
        if err is not None:
            self.failure_count += 1
            logger.error("Message delivery failed: %s", err)
        else:
            self.success_count += 1
            key = msg.key().decode("utf-8") if msg.key() else "None"
            logger.info(
                "Delivered to topic '%s' [partition %d] at offset %d | Key (txId): %s",
                msg.topic(),
                msg.partition(),
                msg.offset(),
                key,
            )

    def publish_transaction(
        self,
        transaction: Dict[str, Any],
        on_delivery: Optional[Callable[[Optional[KafkaError], Any], None]] = None,
    ) -> None:
        """Publish a single transaction event with transactionId as the message key."""
        callback = on_delivery or self._delivery_report
        payload = json.dumps(transaction).encode("utf-8")
        # Transaction ID as the Kafka message key
        key = str(transaction["transactionId"]).encode("utf-8")

        self._producer.produce(
            topic=self.topic,
            key=key,
            value=payload,
            on_delivery=callback,
        )
        self._producer.poll(0)

    def flush(self, timeout: float = 10.0) -> int:
        """Flush pending messages to broker."""
        logger.info("Flushing pending messages (timeout=%.1fs)...", timeout)
        remaining = self._producer.flush(timeout=timeout)
        logger.info(
            "Flush complete. Total published: %d, Failures: %d, Remaining in queue: %d",
            self.success_count,
            self.failure_count,
            remaining,
        )
        return remaining

    def close(self) -> None:
        """Close producer cleanly."""
        self.running = False
        self.flush()


def run_producer(
    bootstrap_servers: str,
    topic: str,
    count: int = 10,
    interval: float = 1.0,
    scenario: str = "normal",
    seed: Optional[int] = None,
) -> int:
    """Execute producer loop for specified count and scenario. Returns total published count."""
    producer = FinStreamProducer(bootstrap_servers=bootstrap_servers, topic=topic)
    generator = TransactionGenerator(seed=seed)

    def handle_sigint(sig, frame):
        logger.warning("Interrupt signal received (Ctrl+C). Initiating shutdown...")
        producer.running = False

    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)

    logger.info(
        "Starting producer loop: count=%d, interval=%.2fs, scenario='%s', topic='%s'",
        count,
        interval,
        scenario,
        topic,
    )

    produced = 0
    try:
        while producer.running and (count <= 0 or produced < count):
            if scenario == "normal":
                tx = generator.generate_transaction()
                producer.publish_transaction(tx)
                produced += 1
                logger.info(
                    "[%d/%s] Published NORMAL tx %s (Amount: %.2f %s, Merchant: %s)",
                    produced,
                    count if count > 0 else "∞",
                    tx["transactionId"],
                    tx["amount"],
                    tx["currency"],
                    tx["merchant"],
                )

            elif scenario == "high-value":
                tx = generator.generate_high_value_transaction()
                producer.publish_transaction(tx)
                produced += 1
                logger.info(
                    "[%d/%s] Published HIGH_VALUE tx %s (Amount: %.2f %s)",
                    produced,
                    count if count > 0 else "∞",
                    tx["transactionId"],
                    tx["amount"],
                    tx["currency"],
                )

            elif scenario == "duplicate":
                pair = generator.generate_duplicate_pair()
                for tx in pair:
                    producer.publish_transaction(tx)
                    produced += 1
                    logger.info(
                        "[%d/%s] Published DUPLICATE tx %s (Account: %s)",
                        produced,
                        count if count > 0 else "∞",
                        tx["transactionId"],
                        tx["accountId"],
                    )
                    time.sleep(0.2)

            elif scenario == "mixed":
                roll = generator._rng.random()
                if roll < 0.75:
                    tx = generator.generate_transaction()
                    producer.publish_transaction(tx)
                    produced += 1
                elif roll < 0.90:
                    tx = generator.generate_high_value_transaction()
                    producer.publish_transaction(tx)
                    produced += 1
                else:
                    pair = generator.generate_duplicate_pair()
                    for ptx in pair:
                        producer.publish_transaction(ptx)
                        produced += 1
                        time.sleep(0.1)

            if producer.running and (count <= 0 or produced < count):
                time.sleep(interval)

    finally:
        remaining = producer.flush()
        producer.close()

    if remaining > 0 or producer.failure_count > 0:
        logger.error("Producer finished with errors: %d failed, %d unconfirmed", producer.failure_count, remaining)
        return 1
    return 0


def main() -> None:
    root_env = Path(__file__).resolve().parent.parent / ".env"
    if root_env.exists():
        load_dotenv(dotenv_path=root_env)
    else:
        load_dotenv()

    default_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    default_topic = os.getenv("KAFKA_TOPIC_TRANSACTIONS", "transactions")

    parser = argparse.ArgumentParser(description="FinStream Kafka Transaction Producer")
    parser.add_argument(
        "--bootstrap-servers",
        default=default_servers,
        help=f"Kafka bootstrap servers (default: {default_servers})",
    )
    parser.add_argument(
        "--topic",
        default=default_topic,
        help=f"Target Kafka topic (default: {default_topic})",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of transactions to publish (default: 10, set <=0 for continuous)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=1.0,
        help="Delay in seconds between transactions (default: 1.0)",
    )
    parser.add_argument(
        "--scenario",
        choices=["normal", "high-value", "duplicate", "mixed"],
        default="normal",
        help="Transaction scenario (default: normal)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Random seed for deterministic outputs",
    )

    args = parser.parse_args()
    exit_code = run_producer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        count=args.count,
        interval=args.interval,
        scenario=args.scenario,
        seed=args.seed,
    )
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
