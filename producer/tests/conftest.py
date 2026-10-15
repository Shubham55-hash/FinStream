"""Shared test fixtures for FinStream producer tests."""
import pytest
from transaction_generator import TransactionGenerator


@pytest.fixture
def generator():
    return TransactionGenerator(seed=42)
