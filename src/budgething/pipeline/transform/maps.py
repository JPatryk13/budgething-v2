from copy import deepcopy
from datetime import datetime
from typing import Any
from budgething.pipeline.models import (
    Currency,
    Account,
)


def _str_to_float(value: str) -> float:
    normalized = value.replace(" ", "").replace(",", ".")
    return float(normalized if normalized != "" else 0.0)


def _get_transaction_date(start: str, end: str, *, date_format: str) -> datetime:
    return min(datetime.strptime(start, date_format), datetime.strptime(end, date_format))


def map_pekao24_data(
    transaction: dict[str, Any],
) -> dict[str, Any] | None:
    """Pekao24 CSV record has following columns:
    - Data księgowania (e.g. 01.05.2025)
    - Data waluty (e.g. 01.05.2025)
    - Nadawca / Odbiorca (e.g. SFD SA)
    - Adres nadawcy / odbiorcy (e.g. www.sklep.sfd.p Glogowska 41,Opole)
    - Rachunek źródłowy (e.g. 1234 5678 9012 3456 7890 1234 56)
    - Rachunek docelowy (e.g. 1234 5678 9012 3456 7890 1234 56)
    - Tytułem (e.g. BLIK REF 123456789012)
    - Kwota operacji (e.g. -235,62)
    - Waluta (e.g. PLN)
    - Numer referencyjny (e.g. C123456789012345678)
    - Typ operacji (e.g. PŁATNOŚĆ BLIK)
    - Kategoria (e.g. Bez kategorii)

    **Warning**: Record doesn't contain the balance.
    """
    if transaction.get("Data księgowania") == "":
        return None

    transaction = deepcopy(transaction)

    date = _get_transaction_date(
        transaction.pop("Data waluty"),
        transaction.pop("Data księgowania"),
        date_format="%d.%m.%Y",
    )
    amount = _str_to_float(transaction.pop("Kwota operacji"))
    currency = Currency(transaction.pop("Waluta").upper())
    category = transaction.pop("Kategoria")
    pmt_type = transaction.pop("Typ operacji")
    description = transaction
    account = Account.PEKAO24

    return {
        "date": date,
        "amount": amount,
        "description": description,
        "currency": currency,
        "account": account,
        "category": category,
        "pmt_type": pmt_type,
    }


def map_revolut_data(transaction: dict[str, Any]) -> dict[str, Any] | None:
    """Revolut CSV record has following columns:
    - Type (e.g. CARD_PAYMENT)
    - Product (e.g. Current)
    - Started Date (e.g. 2024-05-04 22:31:29)
    - Completed Date (e.g. 2024-05-05 11:55:42)
    - Description (e.g. Tesco)
    - Amount (e.g. -10.04)
    - Fee (e.g. 0.00)
    - Currency (e.g. GBP)
    - State (e.g. COMPLETED)
    - Balance (e.g. 758.72)

    **Warning**: Record doesn't contain the category.
    """
    if transaction.get("State", None) == "REVERTED" or transaction.get("Completed Date") == "":
        return None

    transaction = deepcopy(transaction)

    date = _get_transaction_date(
        transaction.pop("Started Date"),
        transaction.pop("Completed Date"),
        date_format="%Y-%m-%d %H:%M:%S",
    )
    amount = _str_to_float(transaction.pop("Amount"))
    balance = _str_to_float(transaction.pop("Balance"))
    currency = Currency(transaction.pop("Currency"))
    account = Account("revolut")
    pmt_type = transaction.pop("Type")
    description = transaction

    return {
        "date": date,
        "amount": amount,
        "balance": balance,
        "description": description,
        "currency": currency,
        "account": account,
        "pmt_type": pmt_type,
    }
