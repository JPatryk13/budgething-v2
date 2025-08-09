from enum import StrEnum


class Currency(StrEnum):
    PLN = "PLN"
    GBP = "GBP"
    EUR = "EUR"


class Account(StrEnum):
    PEKAO24 = "pekao24"
    REVOLUT = "revolut"
