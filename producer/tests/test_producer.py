"""
Unit tests for FinStreamProducer using mocked Kafka Producer.
"""

import json
from unittest.mock import MagicMock
import pytest
from producer import FinStreamProducer, run_producer


def test_producer_initialization():
    mock_inner = MagicMock()
    producer = FinStreamProducer(
        bootstrap_servers="localhost:9092",
        topic="transactions",
        producer_instance=mock_inner,
    )
    assert producer.topic == "transactions"
    assert producer.bootstrap_servers == "localhost:9092"


def test_publish_transaction_payload_and_key():
    mock_inner = MagicMock()
    producer = FinStreamProducer(
        bootstrap_servers="localhost:9092",
        topic="transactions",
        producer_instance=mock_inner,
    )

    sample_tx = {
        "transactionId": "TX1001",
        "accountId": "ACC101",
        "amount": 2500.00,
        "currency": "INR",
        "timestamp": "2026-10-15T12:30:00Z",
        "merchant": "Demo Store",
    }

    producer.publish_transaction(sample_tx)

    mock_inner.produce.assert_called_once()
    args, kwargs = mock_inner.produce.call_args

    assert kwargs["topic"] == "transactions"
    # Ensure transactionId is used as message key
    assert kwargs["key"] == b"TX1001"
    payload = json.loads(kwargs["value"].decode("utf-8"))
    assert payload["transactionId"] == "TX1001"
    assert payload["accountId"] == "ACC101"
    assert payload["amount"] == 2500.00
    assert payload["merchant"] == "Demo Store"

    mock_inner.poll.assert_called_once_with(0)


def test_delivery_report_success():
    mock_inner = MagicMock()
    producer = FinStreamProducer(producer_instance=mock_inner)

    mock_msg = MagicMock()
    mock_msg.topic.return_value = "transactions"
    mock_msg.partition.return_value = 0
    mock_msg.offset.return_value = 10
    mock_msg.key.return_value = b"TX1001"

    producer._delivery_report(None, mock_msg)
    assert producer.success_count == 1
    assert producer.failure_count == 0


def test_delivery_report_failure():
    mock_inner = MagicMock()
    producer = FinStreamProducer(producer_instance=mock_inner)

    mock_err = MagicMock()
    mock_err.__str__.return_value = "Broker: Message timed out"

    mock_msg = MagicMock()
    producer._delivery_report(mock_err, mock_msg)
    assert producer.success_count == 0
    assert producer.failure_count == 1


def test_flush_and_close():
    mock_inner = MagicMock()
    mock_inner.flush.return_value = 0
    producer = FinStreamProducer(producer_instance=mock_inner)

    remaining = producer.flush(timeout=5.0)
    assert remaining == 0
    mock_inner.flush.assert_called_once_with(timeout=5.0)

    producer.close()
    assert not producer.running
