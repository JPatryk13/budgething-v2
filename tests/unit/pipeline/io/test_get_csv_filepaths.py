from pathlib import Path
from budgething.pipeline.io import get_csv_filepaths


def test_get_csv_filepaths_returns_all_csv_when_no_prefix_given(
    tmp_path: Path,
):
    # GIVEN I have a directory with multiple CSV files
    csv_filenames = ["file1.csv", "file2.csv", "file3.csv"]
    non_csv_filenames = ["file1.txt", "file2.doc", "file3.pdf"]
    for filename in csv_filenames + non_csv_filenames:
        (tmp_path / filename).touch()

    # WHEN I call get_csv_filepaths with no prefix
    filepaths = get_csv_filepaths(tmp_path)

    # THEN I should get all CSV file paths in the directory
    assert set(filepaths) == {tmp_path / fn for fn in csv_filenames}


def test_get_csv_filepaths_returns_specific_csv_with_prefix(
    tmp_path: Path,
):
    # GIVEN I have a directory with multiple CSV files
    prefixed_csv_filenames = [
        "bar-file1.csv",
        "bar_file2.csv",
        "barfile3.csv",
    ]
    non_prefixed_csv_filenames = ["file1.txt", "file2.doc", "file3.pdf"]
    for filename in prefixed_csv_filenames + non_prefixed_csv_filenames:
        (tmp_path / filename).touch()

    # WHEN I call get_csv_filepaths with no prefix
    filepaths = get_csv_filepaths(tmp_path, prefix="bar")

    # THEN I should get all CSV file paths in the directory
    assert set(filepaths) == {tmp_path / fn for fn in prefixed_csv_filenames}
