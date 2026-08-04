"""
logging.py - sets up root logger and handler and configures the formatter

Assumptions:
    1. PipelineFormatter exists to handle third-party loggers (like Snowflake's)
       that don't have run_id in their records (logging format)
"""
import logging
import os
from utils.paths import LOGS_DIR
import sys

class PipelineFormatter(logging.Formatter):
    def format(self, record):
        if "run_id" not in record.__dict__:
            record.run_id = "external"
        return super().format(record)

def get_logger(name: str, run_id: str) -> logging.LoggerAdapter:
    root = logging.getLogger()

    if not root.handlers:
        os.makedirs(LOGS_DIR, exist_ok=True)

        file_handler = logging.FileHandler(LOGS_DIR / "pipeline.log")
        stream_handler = logging.StreamHandler(sys.stdout)
        formatter = PipelineFormatter(
            "%(asctime)s - %(run_id)s - %(funcName)s - %(levelname)s - %(message)s"
        )

        file_handler.setFormatter(formatter)
        file_handler.setLevel(logging.INFO)
        root.addHandler(file_handler)

        stream_handler.setFormatter(formatter)
        stream_handler.setLevel(logging.INFO)
        root.addHandler(stream_handler)

        root.setLevel(logging.INFO)

    logger = logging.getLogger(name)
    adapter = logging.LoggerAdapter(logger, {"run_id": run_id})

    return adapter





