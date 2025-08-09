from datetime import datetime

import pandas as pd
from budgething.pipeline.transform.parsing import (
    _map_revolut_data,
)
from budgething.pipeline.models import (
    Currency,
    Account,
)


def test_map_revolut_data():
    data = pd.DataFrame(
        [
            {
                "Type": "EXCHANGE",
                "Product": "Current",
                "Started Date": "2024-11-05 12:01:02",
                "Completed Date": "2024-11-06 14:37:47",
                "Description": "Exchange to PLN",
                "Amount": "18.34",
                "Fee": "0.00",
                "Currency": "PLN",
                "State": "COMPLETED",
                "Balance": "518.40",
            }
        ]
    )

    expected = pd.DataFrame(
        [
            {
                "date": datetime(2024, 11, 5, 12, 1, 2),
                "amount": 18.34,
                "currency": Currency.PLN,
                "account": Account.REVOLUT,
                "balance": 518.40,
                "payment_type": "EXCHANGE",
            }
        ]
    )
    result = _map_revolut_data(data)
    pd.testing.assert_frame_equal(result, expected, check_like=True)


def test_map_revolut_data_handles_reverted_transaction() -> None:
    reverted_transaction = {
        "Type": "CARD_PAYMENT",
        "Product": "Current",
        "Started Date": "2024-11-05 12:01:02",
        "Completed Date": "",
        "Description": "Tesco",
        "Amount": "-10.04",
        "Fee": "0.00",
        "Currency": "GBP",
        "State": "REVERTED",
        "Balance": "",
    }
    completed_transaction = {
        "Type": "CARD_PAYMENT",
        "Product": "Current",
        "Started Date": "2024-11-05 12:01:02",
        "Completed Date": "2024-11-05 21:01:02",
        "Description": "Tesco",
        "Amount": "-10.04",
        "Fee": "0.00",
        "Currency": "GBP",
        "State": "COMPLETED",
        "Balance": "758.72",
    }
    data = pd.DataFrame([reverted_transaction, completed_transaction])

    pd.testing.assert_frame_equal(_map_revolut_data(data), pd.DataFrame([completed_transaction]))
