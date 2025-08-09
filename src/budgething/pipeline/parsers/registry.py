from typing import Callable

import pandas as pd

Parser = Callable[[pd.DataFrame], pd.DataFrame]
PARSER_REGISTRY: list[tuple[set[str], Parser]] = []


def _register_parser(required_fields: set[str]) -> Callable:
    """Decorator to register a parser function with its required fields."""

    def decorator(func: Parser) -> Parser:
        PARSER_REGISTRY.append((required_fields, func))
        return func

    return decorator
