import csv
from datetime import datetime
from pathlib import Path
import pickle

from budgething.pipeline.io import get_csv_filepaths
from budgething.pipeline.transform.maps import (
    map_pekao24_data,
    map_revolut_data,
)
from budgething.pipeline.models import Transaction

DATA_PATH = Path(__file__).resolve().parent.parent.parent.parent / "data"


def csv_to_pkl(csvdir: Path = DATA_PATH / "input", pkldir: Path | None = None) -> Path:
    data = []
    filepaths = get_csv_filepaths(csvdir)

    print(f"Found {len(filepaths)} files in {csvdir}")

    for filepath in filepaths:
        with open(filepath, "r", encoding="utf-8") as file:
            file_content = csv.DictReader(
                file, delimiter=";" if "pekao24" in filepath.name else ","
            )
            for i, row in enumerate(file_content):
                row = {k.strip(): v.strip() for k, v in row.items()}
                try:
                    processed = (
                        map_pekao24_data(row)
                        if "pekao24" in filepath.name
                        else map_revolut_data(row)
                    )
                except (KeyError, ValueError) as e:
                    raise Exception(f"{e} in file {filepath.name} at line {i + 1}: {row=}") from e

                if processed is not None:
                    data.append(processed)

    print(f"Loaded {len(data)} transactions")

    pkldir = pkldir or (
        DATA_PATH / "pickles" / f"transactions-{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.pkl"
    )

    with open(pkldir, "wb") as pkl:
        pickle.dump(data, pkl)

    print(f"Saved {len(data)} ({pkldir.stat().st_size / 1024:.2f} KB) transactions to {pkldir}")

    with open(pkldir, "rb") as pkl:
        loaded_data: list[Transaction] = pickle.load(pkl)

    print(f"Loaded {len(loaded_data)} transactions from {pkldir}")

    return pkldir


if __name__ == "__main__":
    csv_to_pkl()
