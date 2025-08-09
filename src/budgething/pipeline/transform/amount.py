from typing import Any
import pandas as pd


def aggregate_daily_net_amounts(data: pd.DataFrame) -> pd.DataFrame:
    """Daily net amount spent/earned from "date" and "amount" columns in the DataFrame.

    ## Example

    >>> data = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=8, freq="9h"),
            "amount": [100, -50, 200, -100, 50, 175, -75, 25],
        }
    )
    >>> print(data)
    '''
                    date  amount
    0 2023-01-01 00:00:00     100
    1 2023-01-01 09:00:00     -50
    2 2023-01-01 18:00:00     200
    3 2023-01-02 03:00:00    -100
    4 2023-01-02 12:00:00      50
    5 2023-01-02 21:00:00     175
    6 2023-01-03 06:00:00     -75
    7 2023-01-03 15:00:00      25
    '''
    >>> print(get_daily_net_amount(data))
    '''
    date
    2023-01-01    250
    2023-01-02    125
    2023-01-03    -50
    Name: daily_net_amount, dtype: int64
    '''
    """
    data = data[["date", "amount"]].copy()
    data["date"] = data["date"].dt.date
    return data.groupby("date").sum().rename(columns={"amount": "daily_net_amount"})


def reindex_with_defaults(
    data: pd.DataFrame, *, fill_value: Any, index_range: tuple[Any, Any] | None = None
) -> pd.DataFrame:
    """Ensure the DataFrame has a continuous date index by filling rows for missing dates with
    a default value.

    Args:
        data (pd.DataFrame):
            DataFrame with a DateTimeIndex.
        fill_value (Any):
            The value to fill for missing dates.
        index_range (tuple[Any, Any] | None):
            Optional range to limit the reindexing. If None, uses the min and max dates from the
            DataFrame.
    """
    if index_range is None:
        index_range = (data.index.min(), data.index.max())
    full_range = pd.date_range(*index_range, freq="D")
    df_filled = data.reindex(full_range, fill_value=fill_value)
    df_filled.index.name = data.index.name or "date"
    return df_filled
