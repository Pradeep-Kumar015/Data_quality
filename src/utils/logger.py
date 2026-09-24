import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a DQM logger.

    The logger writes to stdout and does not propagate to the
    root logger. This prevents duplicate log messages when the
    DQM framework runs as a subprocess inside Airflow.
    """

    logger = logging.getLogger(name)

    # --------------------------------------------------------
    # Prevent duplicate log messages
    # --------------------------------------------------------
    logger.propagate = False

    # --------------------------------------------------------
    # Configure handler only once
    # --------------------------------------------------------
    if not logger.handlers:

        handler = logging.StreamHandler(
            sys.stdout
        )

        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        )

        handler.setFormatter(
            formatter
        )

        logger.addHandler(
            handler
        )

    # --------------------------------------------------------
    # Set logging level
    # --------------------------------------------------------
    logger.setLevel(
        logging.INFO
    )

    return logger