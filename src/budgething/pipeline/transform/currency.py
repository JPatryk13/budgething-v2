from currency_converter import CurrencyConverter  # type: ignore[import-untyped]
import pandas as pd

from budgething.pipeline.models import Currency


def convert_currency(df: pd.DataFrame, target_currency: Currency) -> pd.DataFrame:
    c = CurrencyConverter(fallback_on_missing_rate=True)
    df["amount"] = df.apply(
        lambda row: c.convert(row["amount"], row["currency"], target_currency, row["date"].date()),
        axis=1,
    )
    df["currency"] = target_currency.value
    return df
