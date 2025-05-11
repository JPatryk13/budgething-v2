from datetime import datetime

from typing import Any
import pytest
from budgething.pipeline.transform.enrichment import add_balance


def get_input_entry(start_day: int, amount: float) -> dict[str, Any]:
    return {
        "date": datetime(2025, 1, start_day),
        "amount": amount,
    }


def get_output_entry(start_day: int, amount: float, balance: float) -> dict[str, Any]:
    return {
        "date": datetime(2025, 1, start_day),
        "amount": amount,
        "balance": balance,
    }


@pytest.mark.parametrize(
    "data,balance_after,expected",
    [
        pytest.param([], 100, [], id="empty"),
        pytest.param(
            [get_input_entry(1, 50)],
            100,
            [get_output_entry(1, 50, 100)],
            id="single entry",
        ),
        pytest.param(
            [get_input_entry(1, -20), get_input_entry(2, -13.5)],
            100,
            [get_output_entry(1, -20, 113.5), get_output_entry(2, -13.5, 100)],
            id="multiple entries",
        ),
        pytest.param(
            [
                get_input_entry(1, -20),
                get_input_entry(2, -13.5),
                get_input_entry(10, 152.5),
            ],
            100,
            [
                get_output_entry(1, -20, -39),
                get_output_entry(2, -13.5, -52.5),
                get_output_entry(10, 152.5, 100),
            ],
            id="multiple unsorted entries",
        ),
    ],
)
def test_add_balance(data, balance_after, expected):
    result = add_balance(data, balance_after)
    assert sorted(
        result,
        key=lambda x: x["date"],
    ) == sorted(
        expected,
        key=lambda x: x["date"],
    )


def test_add_balance_missing_keys_from_data():
    data = [{"date": "2023-01-01"}]
    with pytest.raises(ValueError):
        add_balance(data, 100)
