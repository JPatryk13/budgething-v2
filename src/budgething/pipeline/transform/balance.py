import pandas as pd


def extract_eod_balance(data: pd.DataFrame) -> pd.Series:
    """End of day balance from "date" and "balance" columns.

    ## Example

    >>> data = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=8, freq="9h"),
            "balance": [100, 50, 200, 100, 150, 325, 250, 275],
        }
    )
    >>> print(data)
    '''
                     date  balance
    0 2023-01-01 00:00:00      100
    1 2023-01-01 09:00:00       50
    2 2023-01-01 18:00:00      200
    3 2023-01-02 03:00:00      100
    4 2023-01-02 12:00:00      150
    5 2023-01-02 21:00:00      325
    6 2023-01-03 06:00:00      250
    7 2023-01-03 15:00:00      275
    '''
    >>> print(get_eod_balance_from_balance(data))
    '''
    date
    2023-01-01    200
    2023-01-02    325
    2023-01-03    275
    Name: eod_balance, dtype: int64
    '''
    """
    data = data[["date", "balance"]].copy()
    data["date"] = data["date"].dt.date
    return (
        data.sort_values("date")
        .groupby("date")
        .tail(1)
        .set_index("date")["balance"]
        .rename("eod_balance")
    )


def reconstruct_eod_balance(data: pd.DataFrame, latest_balance: float) -> pd.Series:
    """End of day balance from "date" and "daily_net_amount" columns, using the latest balance.
    Treats `latest_balance` as the most recent day EOD balance

    ## Example

    >>> data = pd.DataFrame(
        {
            "date": pd.date_range(start="2023-01-01", periods=8, freq="D"),
            "daily_net_amount": [100, -50, 200, -100, 50, 175, -75, 25],
        }
    )
    >>> print(data)
    '''
            date  daily_net_amount
    0 2023-01-01               100
    1 2023-01-02               -50
    2 2023-01-03               200
    3 2023-01-04              -100
    4 2023-01-05                50
    5 2023-01-06               175
    6 2023-01-07               -75
    7 2023-01-08                25
    '''
    >>> print(get_eod_balance_from_latest_balance(data, 1000))
    '''
    date
    2023-01-01     775
    2023-01-02     725
    2023-01-03     925
    2023-01-04     825
    2023-01-05     875
    2023-01-06    1050
    2023-01-07     975
    2023-01-08    1000
    Name: eod_balance, dtype: int64
    '''
    """
    if data.index.name == "date":
        data = data.reset_index()
        data["date"] = pd.to_datetime(data["date"])
    data = data[["date", "daily_net_amount"]].copy()
    data["date"] = data["date"].dt.date
    data = data.set_index("date", verify_integrity=True).sort_index(ascending=False)
    reverse_cumsum = data["daily_net_amount"].cumsum().shift(fill_value=0)
    return (latest_balance - reverse_cumsum).iloc[::-1].rename("eod_balance")
