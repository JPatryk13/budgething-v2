import logging
from pathlib import Path
from dash import Dash

import pandas as pd

from budgething.config import DATA_PATH, DEBUG
from budgething.pipeline._logging import configure_logging
from budgething.data_io.csv_data_reader import CSVDataReader
from budgething.pipeline.parsers import PARSER_REGISTRY, repair_data_pipeline


configure_logging(level=logging.DEBUG if DEBUG else logging.INFO)

# app = Dash()


def main(csvdir: Path = DATA_PATH / "input"):
    parsed = []

    for schema, parser in PARSER_REGISTRY:
        reader = CSVDataReader(csvdir, schema=schema, skip_hashes=set())
        data = reader.get_all(add_meta=True, strip=True)
        parsed.append(parser(data))

    print(parsed)

    # app.layout = []
    # eod_balance = {}

    # for df in parsed:
    #     converted = convert_currency(df, Currency.PLN)
    #     eod_balance[account] = pd.DataFrame(
    #         get_eod_balance_from_latest_balance(converted, latest_balance=4158.68)
    #         if account == Account.PEKAO24
    #         else get_eod_balance_from_known_balance(converted)
    #     )

    # total = pd.DataFrame(sum_eod_balances(eod_balance.values())).sort_index().reset_index()
    # logging.warning(f"Data: {total.columns}")
    # app.layout += [
    #     dcc.Graph(
    #         figure=px.line(
    #             total,
    #             x="date",
    #             y=total.columns,
    #         )
    #     ),
    # ]


if __name__ == "__main__":
    # 1. Update
    #   a) Read any new* files given as input
    #   b) Process the data
    #   c) Read most recent processed data if there are any
    #   d) Filter to get only new data
    #   e) Write new data to storage

    # 2. Presentation
    #   a) Read data from storage
    #   b) Create plots
    #       - "📈 Balance over time per account"
    #       - "💸 Monthly/weekly spending trends"
    #       - "🧾 Category-wise spend breakdown"
    #       - "📍 Recurring vs non-recurring expenses"
    #       - "📉 Net income trend"
    #       - "🧠 Outlier detection or unusual spending alerts"
    #       - "💰 Income vs Expenses over time"
    #
    # * new files can be filtered by calculating their hashes

    main()
    # app.run(debug=DEBUG)
