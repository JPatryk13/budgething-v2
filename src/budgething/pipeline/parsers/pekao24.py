import pandas as pd

from budgething.data_io.csv_data_reader import MetaColName
from budgething.pipeline.models import Account
from budgething.pipeline.parsers.helpers import asfloat, get_date
from budgething.pipeline.parsers.registry import _register_parser


PEKAO24_REQUIRED_FIELDS = {
    "Data księgowania",
    "Data waluty",
    "Kwota operacji",
    "Waluta",
    "Kategoria",
    "Typ operacji",
}


@_register_parser(PEKAO24_REQUIRED_FIELDS)
def _map_pekao24_data(transactions: pd.DataFrame) -> pd.DataFrame:

    df = pd.DataFrame(transactions)

    df["date"] = get_date(df[["Data waluty", "Data księgowania"]], "%d.%m.%Y")
    df["amount"] = asfloat(df["Kwota operacji"])
    df["currency"] = df["Waluta"].str.upper()
    df["category"] = df["Kategoria"]
    df["payment_type"] = df["Typ operacji"]
    df = df.assign(account=Account.PEKAO24)

    return df[
        [
            "date",
            "amount",
            "currency",
            "category",
            "payment_type",
            "account",
            *MetaColName.astuple(),
        ]
    ]
