from datetime import datetime
from enum import StrEnum
from typing import Any
from pydantic import BaseModel


class Currency(StrEnum):
    PLN = "PLN"
    GBP = "GBP"
    EUR = "EUR"


class Account(StrEnum):
    PEKAO24 = "pekao24"
    REVOLUT = "revolut"


class Transaction(BaseModel):
    date: datetime
    amount: float
    balance_after: float
    description: Any
    currency: Currency
    account: Account
    category: str | None = None
    pmt_type: str | None = None
