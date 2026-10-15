"""
Unit tests for FinStream TransactionGenerator.
"""

from decimal import Decimal
import pytest
from transaction_generator import TransactionGenerator, SUPPORTED_CURRENCIES, DEFAULT_MERCHANTS


def test_generate_single_transaction_valid():
    gen = TransactionGenerator(seed=42)
    tx = gen.generate_transaction()

    assert isinstance(tx, dict)
    assert tx["transactionId"].startswith("TX")
    assert tx["accountId"].startswith("ACC")
    assert tx["amount"] > 0
    assert tx["currency"] == "INR"
    assert tx["merchant"] in DEFAULT_MERCHANTS
    assert tx["timestamp"].endswith("Z")


def test_seed_reproducibility():
    gen1 = TransactionGenerator(seed=999)
    gen2 = TransactionGenerator(seed=999)

    tx1 = gen1.generate_transaction()
    tx2 = gen2.generate_transaction()

    assert tx1["transactionId"] == tx2["transactionId"]
    assert tx1["accountId"] == tx2["accountId"]
    assert tx1["amount"] == tx2["amount"]
    assert tx1["merchant"] == tx2["merchant"]


def test_generate_high_value_transaction():
    gen = TransactionGenerator(seed=123)
    tx = gen.generate_high_value_transaction()

    assert tx["amount"] > 50000.0
    TransactionGenerator.validate(tx)


def test_generate_duplicate_pair():
    gen = TransactionGenerator(seed=55)
    tx1, tx2 = gen.generate_duplicate_pair()

    assert tx1["transactionId"] == tx2["transactionId"]
    assert tx1["accountId"] == tx2["accountId"]
    assert tx1["amount"] == tx2["amount"]
    TransactionGenerator.validate(tx1)
    TransactionGenerator.validate(tx2)


def test_validation_missing_field():
    tx = {
        "transactionId": "TX1001",
        "accountId": "ACC101",
        "amount": 2500.00,
        "currency": "INR",
        # missing timestamp and merchant
    }
    with pytest.raises(ValueError, match="Missing required field"):
        TransactionGenerator.validate(tx)


def test_validation_blank_identifiers():
    gen = TransactionGenerator()
    with pytest.raises(ValueError, match="transactionId must be a non-empty string"):
        gen.generate_transaction(transaction_id="   ")

    with pytest.raises(ValueError, match="accountId must be a non-empty string"):
        gen.generate_transaction(account_id="")

    with pytest.raises(ValueError, match="merchant must be a non-empty string"):
        gen.generate_transaction(merchant="")


def test_validation_non_positive_amount():
    gen = TransactionGenerator()
    with pytest.raises(ValueError, match="amount must be a positive decimal number"):
        gen.generate_transaction(amount=-100.0)

    with pytest.raises(ValueError, match="amount must be a positive decimal number"):
        gen.generate_transaction(amount=0.0)


def test_validation_unsupported_currency():
    gen = TransactionGenerator()
    with pytest.raises(ValueError, match="Invalid or unsupported currency"):
        gen.generate_transaction(currency="USD")


def test_validation_invalid_timestamp():
    tx = {
        "transactionId": "TX1001",
        "accountId": "ACC101",
        "amount": 2500.00,
        "currency": "INR",
        "timestamp": "2026-10-15 12:30:00",  # missing T and Z
        "merchant": "Demo Store",
    }
    with pytest.raises(ValueError, match="Invalid timestamp"):
        TransactionGenerator.validate(tx)
