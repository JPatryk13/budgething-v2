from datetime import datetime

import pytest
from budgething.pipeline.transform.maps import map_pekao24_data
from budgething.pipeline.models import (
    Currency,
    Account,
)


@pytest.mark.parametrize(
    "field_name",
    [
        "date",
        "amount",
        "description",
        "currency",
        "account",
        "category",
        "pmt_type",
    ],
)
def test_map_pekao24_data(field_name: str) -> None:
    data = {
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

    expected = dict(
        date=datetime(2025, 5, 1),
        amount=-235.62,
        description={
            "Nadawca / Odbiorca": "SFD SA",
            "Adres nadawcy / odbiorcy": "www.sklep.sfd.p Glogowska 41,Opole",
            "Rachunek źródłowy": "1234 5678 9012 3456 7890 1234 56",
            "Rachunek docelowy": "1234 5678 9012 3456 7890 1234 56",
            "Tytułem": "BLIK REF 123456789012",
            "Numer referencyjny": "C123456789012345678",
        },
        currency=Currency.PLN,
        account=Account.PEKAO24,
        category="Bez kategorii",
        pmt_type="PŁATNOŚĆ BLIK",
    )
    transaction = map_pekao24_data(data)
    assert transaction[field_name] == expected[field_name]


@pytest.mark.parametrize(
    "data,expected_description",
    [
        pytest.param(
            {
                "Data księgowania": "02.05.2025",
                "Data waluty": "01.05.2025",
                "Nadawca / Odbiorca": "SFD SA",
                "Adres nadawcy / odbiorcy": "www.sklep.sfd.p Glogowska 41,Opole",
                "Rachunek źródłowy": "1234 5678 9012 3456 7890 1234 56",
                "Rachunek docelowy": "1234 5678 9012 3456 7890 1234 56",
                "Kwota operacji": "-235,62",
                "Waluta": "PLN",
                "Numer referencyjny": "C123456789012345678",
                "Typ operacji": "PŁATNOŚĆ BLIK",
                "Kategoria": "Bez kategorii",
            },
            {
                "Nadawca / Odbiorca": "SFD SA",
                "Adres nadawcy / odbiorcy": "www.sklep.sfd.p Glogowska 41,Opole",
                "Rachunek źródłowy": "1234 5678 9012 3456 7890 1234 56",
                "Rachunek docelowy": "1234 5678 9012 3456 7890 1234 56",
                "Numer referencyjny": "C123456789012345678",
            },
            id="missing field",
        ),
        pytest.param(
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
                "Foo": "Bar",
            },
            {
                "Nadawca / Odbiorca": "SFD SA",
                "Adres nadawcy / odbiorcy": "www.sklep.sfd.p Glogowska 41,Opole",
                "Rachunek źródłowy": "1234 5678 9012 3456 7890 1234 56",
                "Rachunek docelowy": "1234 5678 9012 3456 7890 1234 56",
                "Tytułem": "BLIK REF 123456789012",
                "Foo": "Bar",
                "Numer referencyjny": "C123456789012345678",
            },
            id="extra field",
        ),
    ],
)
def test_map_pekao24_data_handles_missing_and_extra_description_fields(
    data: dict,
    expected_description: dict,
) -> None:
    transaction = map_pekao24_data(data)
    assert transaction["description"] == expected_description
