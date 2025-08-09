import hashlib
from pathlib import Path
import pandas as pd
from pandas.testing import assert_frame_equal
import pytest
from pytest_mock import MockerFixture

from budgething.data_io.csv_data_reader import CSVDataReader, CSVFile


@pytest.fixture(name="csv_file")
def fixture_csv_file_obj(tmp_path: Path) -> tuple[CSVFile, str]:
    csv_file = tmp_path / "test.csv"
    content = "col1,col2\nval1,val2"
    csv_file.write_text(content, encoding="utf-8")
    return CSVFile(csv_file), content


class TestCSVFile:
    def test_csv_file_init_fails_for_non_csv_file(self, mocker: MockerFixture):
        mock_path = mocker.patch("budgething.data_io.csv_data_reader.Path")
        mock_path.suffix = ".csv"
        CSVFile(mock_path)  # OK
        mock_path.suffix = ".CSV"
        CSVFile(mock_path)  # OK
        mock_path.suffix = ".txt"
        with pytest.raises(ValueError):
            CSVFile(mock_path)

    def test_sha256_property(self, csv_file: tuple[CSVFile, str]):
        def ishex(s: str) -> bool:
            return isinstance(s, str) and len(s) == 64 and all(c in "0123456789abcdef" for c in s)

        assert ishex(csv_file[0].sha256)

    def test_dialect_property(self, csv_file: tuple[CSVFile, str]):
        assert csv_file[0].dialect.delimiter == ","

    def test_delimiter_property(self, csv_file: tuple[CSVFile, str]):
        assert csv_file[0].delimiter == ","

    def test_schema_property(self, csv_file: tuple[CSVFile, str]):
        assert csv_file[0].schema == {"col1", "col2"}

    @pytest.mark.parametrize(
        "content, expected, add_meta, strip",
        [
            pytest.param(
                "col1,col2\nval1,val2",
                pd.DataFrame({"col1": ["val1"], "col2": ["val2"]}),
                False,
                False,
                id="basic",
            ),
            pytest.param(
                "col1 ,col2  \nval1  , val2",
                pd.DataFrame({"col1 ": ["val1  "], "col2  ": [" val2"]}),
                False,
                False,
                id="preserves_whitespaces",
            ),
            pytest.param(
                "col1 ,col2\nval1  , val2",
                pd.DataFrame({"col1": ["val1"], "col2": ["val2"]}),
                False,
                True,
                id="removes_whitespaces",
            ),
        ],
    )
    def test_read(
        self,
        tmp_path: Path,
        content: str,
        expected: pd.DataFrame,
        add_meta: bool,
        strip: bool,
    ):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(content, encoding="utf-8")
        obj = CSVFile(csv_file)
        assert_frame_equal(obj.read(add_meta=add_meta, strip=strip), expected, check_like=True)

    def test_read_adds_metadata(self, tmp_path: Path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("col1,col2\nval1,val2", encoding="utf-8")
        obj = CSVFile(csv_file)
        assert_frame_equal(
            obj.read(add_meta=True, strip=False),
            pd.DataFrame(
                {"col1": ["val1"], "col2": ["val2"], "line_number": [0], "source": [csv_file]}
            ),
            check_like=True,
        )


class TestCSVDataReader:
    def test_constructor_raises_for_non_directory_path(self, tmp_path: Path):
        path = tmp_path / "test.csv"
        path.write_text("col1,col2\nval1,val2", encoding="utf-8")
        with pytest.raises(Exception):
            CSVDataReader(path, schema=set(), skip_hashes=set())

    @pytest.mark.parametrize(
        "files,schema,skip_files,expected_filenames",
        [
            pytest.param(
                {"test1.csv": "col1,col2\nval1,val2", "test2.CSV": "col1,col2\nval1,val2"},
                None,
                set(),
                ["test1.csv", "test2.CSV"],
                id="only_csv_files_no_schema_and_skip_hashes",
            ),
            pytest.param(
                {"test1.txt": "", "test2.csv": "col1,col2\nval1,val2"},
                None,
                set(),
                ["test2.csv"],
                id="mix_of_files_no_schema_and_skip_hashes",
            ),
            pytest.param(
                {
                    "test1.csv": "col1,col2\nval1,val2",
                    "test2.csv": "col1,col3\nval1,val2",
                    "test3.csv": "col1,col2,col3\nval1,val2,val3",
                },
                {"col1", "col2"},
                set(),
                ["test1.csv", "test3.csv"],
                id="given_schema_no_skip_hashes",
            ),
            pytest.param(
                {"test1.csv": "col1,col2\nval1,val2", "test2.csv": "col1,col3\nval1,val2"},
                None,
                {"test1.csv"},
                ["test2.csv"],
                id="no_schema_but_skip_hashes_given",
            ),
        ],
    )
    def test_files(
        self,
        tmp_path: Path,
        files: dict[str, str],
        schema: set[str],
        skip_files: set[str],
        expected_filenames: list[str],
    ):
        skip_hashes = set()
        for filename, content in files.items():
            (tmp_path / filename).write_text(content, encoding="utf-8")
            if filename in skip_files:
                skip_hashes.add(hashlib.sha256((tmp_path / filename).read_bytes()).hexdigest())
        obj = CSVDataReader(tmp_path, schema=schema, skip_hashes=skip_hashes)
        assert len(obj.files) == len(expected_filenames)
        for file in obj.files:
            assert file.filepath.name in expected_filenames

    def test_get_all_concatenates_file_contents(self, tmp_path: Path):
        csv1 = tmp_path / "test1.csv"
        csv1.write_text("col1,col2\nval1,val2", encoding="utf-8")
        csv2 = tmp_path / "test2.csv"
        csv2.write_text("col1,col2\nval3,val4", encoding="utf-8")
        obj = CSVDataReader(tmp_path, schema={"col1", "col2"}, skip_hashes=set())
        df = obj.get_all(add_meta=False, strip=False)
        expected_df = pd.DataFrame({"col1": ["val1", "val3"], "col2": ["val2", "val4"]})
        assert_frame_equal(df, expected_df, check_like=True)
