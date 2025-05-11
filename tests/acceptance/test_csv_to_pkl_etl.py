"""This tests the pipeline that loads CSV files, transforms them into Transaction objects, and saves
them as a pickle file."""

import csv
from datetime import datetime
from pathlib import Path

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from budgething.pipeline.runner import csv_to_pkl
import pickle

from tests.conftest import RESOURCES_ROOT


RESOURCES_PATH = RESOURCES_ROOT / "acceptance" / "sample_csv_statements"


def normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert columns to appropriate types and sort the data.

    Args:
        df (pd.DataFrame): The DataFrame to normalize.

    Returns:
        pd.DataFrame: The normalized DataFrame.
    """
    df = df[["date", "amount", "currency"]]
    if hasattr(df["date"], "dt"):
        df["date"] = pd.to_datetime(df["date"].dt.strftime("%Y-%m-%d"), format="%Y-%m-%d")
    else:
        df["date"] = pd.to_datetime(df["date"], format="%Y-%m-%d")
    df["amount"] = df["amount"].astype(float)
    df["currency"] = df["currency"].astype(str)
    return df.sort_values(by=["date", "amount"]).reset_index(drop=True)


def test_csv_to_pkl(tmp_path: Path):
    # GIVEN a directory with CSV files
    input_path = RESOURCES_ROOT / "acceptance" / "sample_csv_statements" / "input"

    # AND an empty output directory
    output_path = tmp_path / "output"

    # AND expected values in the CSV files
    with open(RESOURCES_PATH / "result.csv", "r") as f:
        expected = pd.DataFrame(csv.DictReader(f))
    expected = normalize_dataframe(expected)

    # WHEN the pipeline is run
    output_filepath = csv_to_pkl(csvdir=input_path, pkldir=output_path)

    # THEN the output directory contains a pickle file
    assert output_filepath.exists(), "Output file was not created."

    with open(output_path, "rb") as f:
        data = pickle.load(f)
    actual = normalize_dataframe(pd.DataFrame(data))

    # AND the pickle file has a list of transaction objects/dicts with N records
    assert_frame_equal(expected, actual)
