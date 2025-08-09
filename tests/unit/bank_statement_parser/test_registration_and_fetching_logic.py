from typing import Callable, Generator
import pytest
import inspect
from budgething.pipeline.transform.parsing import (
    _register_parser,
    PARSER_REGISTRY,
)

DUMMY_REQUIRED_FIELDS = {"Foo", "Bar"}


def dummy_parser(transaction):
    return {v: k for k, v in transaction.items()}


@pytest.fixture(name="register_parser")
def fixture_register_parser() -> Generator:

    _register_parser(DUMMY_REQUIRED_FIELDS)(dummy_parser)

    yield

    PARSER_REGISTRY.remove((DUMMY_REQUIRED_FIELDS, dummy_parser))


def test__register_parser(register_parser):
    assert dummy_parser in [func for _, func in PARSER_REGISTRY]
