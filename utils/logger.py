"""Logging configuration."""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

from config.settings import settings


def setup_logger(
    name: str,
    log_file: Optional[str] = None,
    level: int = logging.INFO,
    include_console: bool = True,
) -> logging.Logger:
    """
    Set up a logger with file and/or console handlers.

    Args:
        name: Logger name
        log_file: Log file name (will be placed in logs directory)
        level: Logging level
        include_console: Whether to also log to console

    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Clear existing handlers
    logger.handlers.clear()

    # Format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # File handler
    if log_file:
        logs_dir = settings.paths.logs_dir
        logs_dir.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(
            logs_dir / log_file,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    # Console handler
    if include_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def get_logger(name: str) -> logging.Logger:
    """Get or create a logger by name."""
    return logging.getLogger(name)


def get_step_logger(step_name: str) -> logging.Logger:
    """
    Get logger for a pipeline step.

    Creates loggers for both step-specific and error logs.

    Args:
        step_name: Name of the pipeline step

    Returns:
        Step logger
    """
    date_str = datetime.now().strftime("%Y-%m-%d")

    # Main step logger
    logger = setup_logger(
        step_name,
        log_file=f"{step_name}_{date_str}.log",
        level=logging.INFO,
    )

    # Error logger
    error_logger = setup_logger(
        f"{step_name}_errors",
        log_file=f"errors_{date_str}.log",
        level=logging.ERROR,
        include_console=False,
    )

    return logger


def get_pipeline_logger() -> logging.Logger:
    """Get main pipeline logger."""
    date_str = datetime.now().strftime("%Y-%m-%d")
    return setup_logger(
        "pipeline",
        log_file=f"pipeline_{date_str}.log",
        level=logging.INFO,
    )
