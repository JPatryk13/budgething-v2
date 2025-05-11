from datetime import datetime

import pytest
from budgething.pipeline.transform.maps import map_revolut_data
from budgething.pipeline.models import (
    Currency,
    Account,
)


@pytest.mark.parametrize(
    "field_name",
    [
        "date",
        "amount",
        "description",
        "currency",
        "account",
        "pmt_type",
        "balance",
    ],
)
def test_map_revolut_data(field_name: str) -> None:
    data = {
        "Type": "EXCHANGE",
        "Product": "Current",
        "Started Date": "2024-11-05 12:01:02",
        "Completed Date": "2024-11-06 14:37:47",
        "Description": "Exchange to PLN",
        "Amount": "18.34",
        "Fee": "0.00",
        "Currency": "PLN",
        "State": "COMPLETED",
        "Balance": "518.40",
    }

    expected = {
        "date": datetime(2024, 11, 5, 12, 1, 2),
        "amount": 18.34,
        "description": {
            "Product": "Current",
            "Description": "Exchange to PLN",
            "Fee": "0.00",
            "State": "COMPLETED",
        },
        "currency": Currency.PLN,
        "account": Account.REVOLUT,
        "balance": 518.40,
        "pmt_type": "EXCHANGE",
    }
    transaction = map_revolut_data(data)
    assert transaction[field_name] == expected[field_name]


@pytest.mark.parametrize(
    "data,expected_description",
    [
        pytest.param(
            {
                "Type": "EXCHANGE",
                "Product": "Current",
                "Started Date": "2024-11-05 12:01:02",
                "Completed Date": "2024-11-06 14:37:47",
                "Description": "Exchange to PLN",
                "Amount": "18.34",
                "Fee": "0.00",
                "Currency": "PLN",
                "Balance": "518.40",
            },
            {
                "Product": "Current",
                "Description": "Exchange to PLN",
                "Fee": "0.00",
            },
            id="missing field",
        ),
        pytest.param(
            {
                "Type": "EXCHANGE",
                "Product": "Current",
                "Started Date": "2024-11-05 12:01:02",
                "Completed Date": "2024-11-06 14:37:47",
                "Description": "Exchange to PLN",
                "Amount": "18.34",
                "Fee": "0.00",
                "Currency": "PLN",
                "State": "COMPLETED",
                "Balance": "518.40",
                "Foo": "Bar",
            },
            {
                "Product": "Current",
                "Description": "Exchange to PLN",
                "Fee": "0.00",
                "State": "COMPLETED",
                "Foo": "Bar",
            },
            id="extra field",
        ),
    ],
)
def test_map_revolut_data_handles_missing_and_extra_description_fields(
    data: dict,
    expected_description: dict,
) -> None:
    transaction = map_revolut_data(data)
    assert transaction["description"] == expected_description


def test_map_revolut_data_handles_reverted_transaction() -> None:
    data = {
        "Type": "CARD_PAYMENT",
        "Product": "Current",
        "Started Date": "2024-11-05 12:01:02",
        "Completed Date": "",
        "Description": "Tesco",
        "Amount": "-10.04",
        "Fee": "0.00",
        "Currency": "GBP",
        "State": "REVERTED",
        "Balance": "758.72",
    }

    assert map_revolut_data(data) is None
