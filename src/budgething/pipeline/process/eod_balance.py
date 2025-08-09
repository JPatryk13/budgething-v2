import logging
from typing import Iterable
import pandas as pd

from budgething.pipeline.transform.amount import aggregate_daily_net_amounts, reindex_with_defaults
from budgething.pipeline.transform.balance import extract_eod_balance, reconstruct_eod_balance
from budgething.pipeline.transform.date import get_max_range


def sum_eod_balances(data: Iterable[pd.DataFrame]) -> pd.Series:
    # Skeleton for all accounts
    max_range = get_max_range(data)
    logging.info(f"Max date range: {max_range[0]} - {max_range[1]}")
    total = pd.DataFrame(
        {"date": pd.date_range(start=max_range[0], end=max_range[1], freq="D")}
    ).set_index("date")

    # Add all accounts to the skeleton
    total = pd.concat(data, axis=1)
    return total.filter(like="eod_balance").sum(axis=1).rename("eod_balance")


def get_eod_balance_from_latest_balance(data: pd.DataFrame, latest_balance: float) -> pd.Series:
    daily = aggregate_daily_net_amounts(data)
    daily = reindex_with_defaults(daily, fill_value=0.0)
    return reconstruct_eod_balance(daily, latest_balance=latest_balance)


def get_eod_balance_from_known_balance(data: pd.DataFrame) -> pd.Series:
    return extract_eod_balance(data).ffill()
