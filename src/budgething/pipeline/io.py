import os
from pathlib import Path


def get_csv_filepaths(
    dirpath: Path, *, prefix: str | None = None
) -> list[Path]:
    filepaths = []
    for root, _, files in os.walk(dirpath):
        filepaths += [
            Path(root) / file
            for file in files
            if file.endswith(".csv")
            and (prefix is None or file.startswith(prefix))
        ]
    return filepaths


###################################################################################################
#                                            New code                                             #
###################################################################################################
from abc import ABC, abstractmethod


class IOAdapter(ABC):

    _file_ext: str

    @abstractmethod
    def _get_filepaths(self, dirpath: Path) -> list[Path]: ...

    @abstractmethod
    def read_all[T](self, filepath: Path, *, model: type[T]) -> list[T]: ...

    @abstractmethod
    def write[T](self, data: list[T], filepath: Path) -> None: ...


class CSV(IOAdapter): ...


class Pickle(IOAdapter): ...
