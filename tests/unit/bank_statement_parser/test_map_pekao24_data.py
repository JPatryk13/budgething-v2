from datetime import datetime

import pandas as pd
import pytest
from budgething.pipeline.transform.parsing import (
    _map_pekao24_data,
)

from budgething.pipeline.models import (
    Currency,
    Account,
)


def test_map_pekao24_data() -> None:
    data = pd.DataFrame(
        [
            {
                "Data księgowania": "02.05.2025",
                "Data waluty": "01.05.2025",
                "Nadawca / Odbiorca": "SFD SA",
                "Adres nadawcy / odbiorcy": "www.sklep.sfd.p Glogowska 41,Opole",
                "Rachunek źródłowy": "1234 5678 9012 3456 7890 1234 56",
                "Rachunek docelowy": "1234 5678 9012 3456 7890 1234 56",
                "Tytułem": "BLIK REF 123456789012",
                "Kwota operacji": "-235,62",
                "Waluta": "PLN",
                "Numer referencyjny": "C123456789012345678",
                "Typ operacji": "PŁATNOŚĆ BLIK",
                "Kategoria": "Bez kategorii",
            }
        ]
    )

    expected = pd.DataFrame(
        [
            {
                "date": datetime(2025, 5, 1),
                "amount": -235.62,
                "currency": Currency.PLN,
                "account": Account.PEKAO24,
                "category": "Bez kategorii",
                "payment_type": "PŁATNOŚĆ BLIK",
            }
        ]
    )
    result = _map_pekao24_data(data)
    pd.testing.assert_frame_equal(expected, result, check_like=True)
