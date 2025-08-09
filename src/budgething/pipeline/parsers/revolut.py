import logging
import pandas as pd

from budgething.data_io.csv_data_reader import MetaColName
from budgething.pipeline.models import Account
from budgething.pipeline.parsers.helpers import asfloat, get_date
from budgething.pipeline.parsers.registry import _register_parser

REVOLUT_REQUIRED_FIELDS = {
    "Started Date",
    "Completed Date",
    "Amount",
    "Balance",
    "Currency",
    "Type",
}


@_register_parser(REVOLUT_REQUIRED_FIELDS)
def _map_revolut_data(transactions: pd.DataFrame) -> pd.DataFrame:

    df = pd.DataFrame(transactions)

    # Remove reverted/cancelled transactions
    df = df[~((df["State"] == "REVERTED") & df["Completed Date"].isna() & df["Balance"].isna())]

    df["date"] = get_date(df[["Started Date", "Completed Date"]], "%Y-%m-%d %H:%M:%S")
    df["amount"] = asfloat(df["Amount"])
    df["balance"] = asfloat(df["Balance"])
    df["currency"] = df["Currency"].str.upper()
    df["payment_type"] = df["Type"]
    df = df.assign(account=Account.REVOLUT)

    return df[
        [
            "date",
            "amount",
            "currency",
            "balance",
            "payment_type",
            "account",
            *MetaColName.astuple(),
        ]
    ]
