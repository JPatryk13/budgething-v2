import pandas as pd


def copy_columns_with_date(
    df: pd.DataFrame, columns: list[str], date_col: str = "date"
) -> pd.DataFrame:
    """
    Copies specified columns from the DataFrame, ensuring the date column
    is included as a regular column (not index), whether it's originally
    the index or a column.

    Args:
        df (pd.DataFrame): Source DataFrame.
        columns (list[str]): List of columns to copy (excluding the date_col).
        date_col (str): Name of the date column or index to ensure is included.

    Returns:
        pd.DataFrame: New DataFrame with the date column and requested columns copied.
    """
    # Check if date_col is index
    if df.index.name == date_col or (date_col == "index" and df.index.name is None):
        # Reset index to bring date_col into columns
        df = df.reset_index()

    # Ensure date_col is present as a column
    if date_col not in df.columns:
        raise KeyError(f"Date column '{date_col}' not found in DataFrame columns or index.")

    # Build list of columns to copy: date_col plus requested columns
    cols_to_copy = [date_col] + columns

    # Copy only existing columns (ignore missing columns gracefully)
    existing_cols = [col for col in cols_to_copy if col in df.columns]

    # Return a copy with just those columns
    return df[existing_cols].copy()
