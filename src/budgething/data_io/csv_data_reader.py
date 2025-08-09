import csv
from enum import Enum
from pathlib import Path
import hashlib
from typing import NamedTuple

import pandas as pd


class UnknownDelimiterError(Exception):
    """Raised when the CSV dialect cannot be determined."""

    def __init__(self, filepath: Path):
        super().__init__(f"Could not determine the delimiter for the file: {filepath}")
        self.filepath = filepath


class InputDirectoryNotFoundError(Exception):
    """Raised when the input directory does not exist or is not a directory."""

    def __init__(self, path: Path):
        super().__init__(f"Path '{path}' does not exist or is not a directory.")
        self.path = path


class MetaColName(Enum):
    """A static class to hold metadata column names."""

    SOURCE = "source"
    LINENO = "line_number"

    @classmethod
    def astuple(cls) -> tuple[str, ...]:
        return cls.SOURCE.value, cls.LINENO.value


class CSVFile:
    def __init__(self, filepath: Path, delimiter: str | None = None, encoding: str = "utf-8"):
        if filepath.suffix.lower() != ".csv":
            raise ValueError(f"Expected a CSV file, got: {filepath}")
        self.filepath = filepath
        self._delimiter = delimiter
        self.encoding = encoding

    @property
    def sha256(self) -> str:
        """Calculate SHA-256 hash of the file content."""
        with open(self.filepath, "rb") as file:
            return hashlib.sha256(file.read()).hexdigest()

    @property
    def dialect(self) -> type[csv.Dialect] | None:
        """Determine the CSV dialect using `csv.Sniffer`."""
        with self.filepath.open("r", encoding="utf-8") as file:
            sample = file.readline()
            file.seek(0)

        if sample.strip() != "":
            try:
                return csv.Sniffer().sniff(sample)
            except csv.Error as e:
                raise OSError(f"Couldn't identify dialect for '{self.filepath}'.") from e

        return None

    @property
    def delimiter(self) -> str:
        """Return the delimiter used in the CSV file."""
        if self._delimiter is not None:
            return self._delimiter
        if (dialect := self.dialect) is not None:
            return dialect.delimiter
        raise UnknownDelimiterError(self.filepath)

    @property
    def schema(self) -> set[str]:
        """Return the schema (column names) of the CSV file."""
        with self.filepath.open("r", encoding=self.encoding) as file:
            reader = csv.reader(file, delimiter=self.delimiter)
            header = next(reader, None)
            if header is None:
                return set()
            return {col.strip() for col in header}

    def read(
        self, *, add_meta: bool, strip: bool, meta_source_col: str, meta_lineno_col: str
    ) -> pd.DataFrame:
        """Read the CSV file content into a DataFrame."""
        with self.filepath.open("r", encoding=self.encoding) as file:
            df = pd.read_csv(file, delimiter=self.delimiter)
            if strip:
                df = self._strip_all(df)
            if add_meta:
                df = self._add_meta(
                    df,
                    meta_source_col=meta_source_col,
                    meta_lineno_col=meta_lineno_col,
                )
            return df

    def _add_meta(
        self,
        data: pd.DataFrame,
        *,
        meta_source_col: str,
        meta_lineno_col: str,
    ) -> pd.DataFrame:
        """Add `line_number` and `source` (where the data came from) columns."""
        return data.assign(**{meta_source_col: self.filepath}).reset_index(names=meta_lineno_col)

    def _strip_all(self, data: pd.DataFrame) -> pd.DataFrame:
        """Strip all string columns (from whitespaces) in the DataFrame."""
        data = data.rename(columns=lambda c: c.strip())
        data = data.apply(lambda col: col.astype(str).str.strip() if col.dtype == "object" else col)
        return data

    def __repr__(self) -> str:
        return (
            f"{self.__class__}(filepath={self.filepath}, delimiter={self.delimiter}, "
            f"encoding={self.encoding})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CSVFile):
            return NotImplemented
        return self.sha256 == other.sha256


class CSVDataReader:

    def __init__(self, target: Path, schema: set[str], skip_hashes: set[str]):
        """
        Args:
            target (Path):
                Path to a directory containing CSV files.
            schema (set[str]):
                Set of column names that the CSV files should contain.
            skip_hashes (set[str]):
                Set of file hashes to skip reading.
        """
        if not target.exists() or not target.is_dir():
            raise InputDirectoryNotFoundError(target)
        self.target = target
        self.schema = schema
        self.skip_hashes = skip_hashes

    @property
    def files(self) -> list[CSVFile]:
        """List of CSVs in the `self.target` directory."""
        files = []
        for path in self.target.iterdir():
            if path.is_file() and path.suffix.lower() == ".csv":
                csv_file = CSVFile(path)
                if csv_file.sha256 not in self.skip_hashes and (
                    self.schema is None or self.schema.issubset(csv_file.schema)
                ):
                    files.append(csv_file)
        return files

    def get_all(
        self,
        *,
        add_meta: bool = False,
        strip: bool = False,
        meta_source_col: str = MetaColName.SOURCE.value,
        meta_lineno_col: str = MetaColName.LINENO.value,
    ) -> pd.DataFrame:
        """Read all CSV files and concatenate them into a single DataFrame."""
        dfs = []
        for csv_file in self.files:
            df = csv_file.read(
                add_meta=add_meta,
                strip=strip,
                meta_source_col=meta_source_col,
                meta_lineno_col=meta_lineno_col,
            )
            if df is not None:
                dfs.append(df)
        result = pd.concat(dfs, axis=0, ignore_index=not add_meta) if dfs else pd.DataFrame()
        return result
