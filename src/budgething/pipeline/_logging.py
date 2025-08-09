import logging
import os
import sys

# For cross-platform color support
import colorama

colorama.init()


class CustomFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[2m",
        "INFO": "\033[38;5;37m",
        "WARNING": "\033[33m",
        "ERROR": "\033[31m",
        "CRITICAL": "\033[1;91m",
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        level = record.levelname
        message = record.getMessage()
        filename = os.path.basename(record.pathname)

        padding = 12  # fixed width target for [LEVEL] + spaces
        pad_spaces = padding - len(level) - 2  # 2 for brackets []
        pad = " " * pad_spaces if pad_spaces > 0 else ""

        color = self.COLORS.get(level, "")
        reset = self.RESET

        if level == "DEBUG":
            formatted = f"[{level}]{pad}{message} ({filename}:{record.lineno})"
        else:
            formatted = f"[{level}]{pad}{message}"

        return f"{color}{formatted}{reset}"


def configure_logging(level: int = logging.DEBUG) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(CustomFormatter())

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers = [handler]
