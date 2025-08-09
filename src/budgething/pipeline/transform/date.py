from typing import Any, Iterable
import pandas as pd


def get_max_range(dfs: Iterable[pd.DataFrame], *, col_name: str | None = None) -> tuple[Any, Any]:
    """Get the maximum date range from a list of DataFrames."""
    min_date = min(df.index.min() if col_name is None else df[col_name].min() for df in dfs)
    max_date = max(df.index.max() if col_name is None else df[col_name].max() for df in dfs)
    return min_date, max_date
