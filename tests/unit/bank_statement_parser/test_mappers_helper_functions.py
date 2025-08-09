from datetime import datetime
import pandas as pd
import pytest
from budgething.pipeline.transform.parsing import _asfloat, _get_date


def test__asfloat():
    assert _asfloat(
        pd.Series(["1 234.56", "1 234,56", "-1234.56", "-1234,56", "0", "0.00", "0,00"])
    ).equals(
        pd.Series([1234.56, 1234.56, -1234.56, -1234.56, 0.0, 0.0, 0.0]),
    )


@pytest.mark.parametrize(
    "data,date_format,expected",
    [
        (
            {
                "Data waluty": ["01.05.2025", "21.05.2025"],
                "Data księgowania": ["02.05.2025", "11.05.2025"],
            },
            "%d.%m.%Y",
            [datetime(2025, 5, 1), datetime(2025, 5, 11)],
        ),
        (
            {
                "Started Date": ["2024-11-05 20:28:30", "2024-11-23 20:28:30"],
                "Completed Date": ["2024-11-06 13:46:04", "2024-11-23 21:28:30"],
            },
            "%Y-%m-%d %H:%M:%S",
            [datetime(2024, 11, 5, 20, 28, 30), datetime(2024, 11, 23, 20, 28, 30)],
        ),
    ],
)
def test__get_date(data: dict, date_format: str, expected: list[datetime]):
    assert _get_date(pd.DataFrame(data), date_format).equals(pd.Series(expected))
