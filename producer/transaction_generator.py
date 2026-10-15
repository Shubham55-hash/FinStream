"""
FinStream - Synthetic Financial Transaction Generator.

Generates realistic and testable financial transaction events in JSON format
adhering to the shared FinStream transaction schema.
"""

from __future__ import annotations

import datetime
from decimal import Decimal, ROUND_HALF_UP
import random
from typing import Any, Dict, List, Optional


SUPPORTED_CURRENCIES = ("INR",)
DEFAULT_MERCHANTS = (
    "Demo Store",
    "Amazon India",
    "Flipkart",
    "Reliance Digital",
    "Swiggy",
    "Zomato",
    "Croma",
)


class TransactionGenerator:
    """Generates synthetic financial transaction events with normal or suspicious patterns."""

    def __init__(self, seed: Optional[int] = None) -> None:
        """Initialize generator with an optional random seed for deterministic testing."""
        self._rng = random.Random(seed)
        self._account_pool = [f"ACC{100 + i}" for i in range(25)]
        self._tx_counter = 1000

    def generate_transaction(
        self,
        transaction_id: Optional[str] = None,
        account_id: Optional[str] = None,
        amount: Optional[Decimal | float | str] = None,
        currency: str = "INR",
        timestamp: Optional[str] = None,
        merchant: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Generate a single transaction event adhering to the shared schema."""
        self._tx_counter += 1
        tx_id = f"TX{self._tx_counter}" if transaction_id is None else transaction_id
        acc_id = self._rng.choice(self._account_pool) if account_id is None else account_id

        if amount is not None:
            amt = Decimal(str(amount)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        else:
            amt = Decimal(str(round(self._rng.uniform(100.0, 15000.0), 2))).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )

        merch = self._rng.choice(DEFAULT_MERCHANTS) if merchant is None else merchant
        ts = timestamp or datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        tx = {
            "transactionId": tx_id,
            "accountId": acc_id,
            "amount": float(amt),
            "currency": currency,
            "timestamp": ts,
            "merchant": merch,
        }
        self.validate(tx)
        return tx

    def generate_high_value_transaction(
        self,
        account_id: Optional[str] = None,
        min_amount: float = 50001.0,
        max_amount: float = 250000.0,
    ) -> Dict[str, Any]:
        """Generate a transaction with amount exceeding high-value threshold (> 50,000 INR)."""
        amt = Decimal(str(round(self._rng.uniform(min_amount, max_amount), 2))).quantize(
            Decimal("0.01"), rounding=ROUND_HALF_UP
        )
        return self.generate_transaction(account_id=account_id, amount=amt)

    def generate_duplicate_pair(self) -> List[Dict[str, Any]]:
        """Generate a pair of transactions sharing the exact same transactionId for idempotency testing."""
        tx1 = self.generate_transaction()
        tx2 = self.generate_transaction(
            transaction_id=tx1["transactionId"],
            account_id=tx1["accountId"],
            amount=tx1["amount"],
            currency=tx1["currency"],
            merchant=tx1["merchant"],
        )
        return [tx1, tx2]

    @staticmethod
    def validate(tx: Dict[str, Any]) -> None:
        """Validate transaction schema and value constraints.

        Raises ValueError if any field is invalid or missing.
        """
        required_fields = (
            "transactionId",
            "accountId",
            "amount",
            "currency",
            "timestamp",
            "merchant",
        )
        for field in required_fields:
            if field not in tx or tx[field] is None:
                raise ValueError(f"Missing required field: '{field}'")

        if not isinstance(tx["transactionId"], str) or not tx["transactionId"].strip():
            raise ValueError("transactionId must be a non-empty string")

        if not isinstance(tx["accountId"], str) or not tx["accountId"].strip():
            raise ValueError("accountId must be a non-empty string")

        if not isinstance(tx["merchant"], str) or not tx["merchant"].strip():
            raise ValueError("merchant must be a non-empty string")

        if not isinstance(tx["amount"], (int, float, Decimal)) or tx["amount"] <= 0:
            raise ValueError(f"amount must be a positive decimal number, got: {tx['amount']}")

        if tx["currency"] not in SUPPORTED_CURRENCIES:
            raise ValueError(
                f"Invalid or unsupported currency '{tx['currency']}'. Supported: {SUPPORTED_CURRENCIES}"
            )

        try:
            ts = tx["timestamp"]
            if not ts.endswith("Z"):
                raise ValueError("timestamp must end with 'Z' for UTC timezone")
            datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")
        except Exception as e:
            raise ValueError(f"Invalid timestamp '{tx['timestamp']}': {e}") from e
