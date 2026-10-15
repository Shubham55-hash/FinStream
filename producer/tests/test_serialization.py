"""
Unit tests for transaction event JSON serialization and deserialization.
"""

from decimal import Decimal
import json
import pytest
from transaction_generator import TransactionGenerator


def test_json_serialization_round_trip():
    gen = TransactionGenerator(seed=101)
    tx = gen.generate_transaction(
        transaction_id="TX_SERIALIZE_01",
        account_id="ACC_SERIALIZE_01",
        amount=12345.67,
        currency="INR",
        timestamp="2026-10-15T12:30:00Z",
        merchant="Demo Store",
    )

    serialized = json.dumps(tx)
    assert isinstance(serialized, str)

    deserialized = json.loads(serialized)
    assert deserialized == tx
    assert deserialized["transactionId"] == "TX_SERIALIZE_01"
    assert deserialized["accountId"] == "ACC_SERIALIZE_01"
    assert deserialized["amount"] == 12345.67
    assert deserialized["currency"] == "INR"
    assert deserialized["timestamp"] == "2026-10-15T12:30:00Z"
    assert deserialized["merchant"] == "Demo Store"


def test_amount_precision_beyond_float():
    gen = TransactionGenerator()
    tx = gen.generate_transaction(amount="9999.99")
    assert tx["amount"] == 9999.99

    payload = json.dumps(tx)
    recovered = json.loads(payload)
    # Beyond JSON boundary: reconstruct Decimal
    decimal_amt = Decimal(str(recovered["amount"]))
    assert decimal_amt == Decimal("9999.99")


def test_required_fields_present_in_serialized_json():
    gen = TransactionGenerator(seed=202)
    tx = gen.generate_transaction()
    serialized = json.dumps(tx)
    data = json.loads(serialized)

    expected_fields = {"transactionId", "accountId", "amount", "currency", "timestamp", "merchant"}
    assert set(data.keys()) == expected_fields
