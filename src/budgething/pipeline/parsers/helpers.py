import pandas as pd


def asfloat(sr: pd.Series) -> pd.Series:
    if not pd.api.types.is_numeric_dtype(sr):
        sr = sr.str.replace(",", ".").str.replace(" ", "").astype(float)
    return sr


def get_date(df: pd.DataFrame, fmt: str) -> pd.Series:
    df = df.copy()
    for col in df.columns:
        df[col] = pd.to_datetime(df[col], format=fmt)
    return df.min(axis=1)
