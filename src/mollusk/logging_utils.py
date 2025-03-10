"""Logging utilities for Mollusk."""

import logging
import sys
from typing import Literal

from mollusk import logger


def configure_logger(
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] | None = None,
    log_file: str | None = None,
    format_string: str | None = None,
) -> None:
    """Configure the Mollusk logger.

    This function can be used by library consumers to configure the Mollusk
    logger according to their needs.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional file path to write logs to
        format_string: Custom format string for log messages
    """
    # set log level if provided
    if level:
        numeric_level = getattr(logging, level)
        logger.setLevel(numeric_level)

    # clear existing handlers
    logger.handlers.clear()

    # create console handler
    console_handler = logging.StreamHandler(sys.stdout)

    # create file handler if log_file is provided
    if log_file:
        file_handler = logging.FileHandler(log_file)
        fmt = format_string or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(fmt)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # add console handler if no handlers exist yet
    if not logger.handlers or log_file is None:
        fmt = format_string or "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        formatter = logging.Formatter(fmt)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)
