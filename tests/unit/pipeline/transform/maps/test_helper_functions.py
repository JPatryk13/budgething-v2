from datetime import datetime
import pytest
from budgething.pipeline.transform.maps import (
    _get_transaction_date,
    _str_to_float,
)


@pytest.mark.parametrize(
    "str_float,expected",
    [
        ("1 234.56", 1234.56),
        ("1 234,56", 1234.56),
        ("-1234.56", -1234.56),
        ("-1234,56", -1234.56),
        ("0", 0.0),
        ("0.00", 0.0),
        ("0,00", 0.0),
    ],
)
def test__str_to_float(str_float: str, expected: float) -> None:
    assert _str_to_float(str_float) == expected


@pytest.mark.parametrize(
    "start_date,end_date,date_format,expected",
    [
        (
            "01.05.2025",
            "02.05.2025",
            "%d.%m.%Y",
            datetime(2025, 5, 1),
        ),
        (
            "2024-11-05 20:28:30",
            "2024-11-06 13:46:04",
            "%Y-%m-%d %H:%M:%S",
            datetime(2024, 11, 5, 20, 28, 30),
        ),
    ],
)
def test__create_transaction_date(start_date, end_date, date_format, expected) -> None:
    assert _get_transaction_date(start_date, end_date, date_format=date_format) == expected
