"""
JSON-formatted logging. Critical for distributed systems — you need to
correlate events across scheduler, workers, and API in one log stream.
"""
import logging
import sys
from pythonjsonlogger import jsonlogger


def setup_logging(service: str, level: str = "INFO") -> logging.Logger:
    logger = logging.getLogger(service)
    logger.setLevel(level)
    if logger.handlers:                 # idempotent — avoid double handlers on reload
        return logger
    handler = logging.StreamHandler(sys.stdout)
    fmt = jsonlogger.JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s",
        rename_fields={"asctime": "ts", "levelname": "level"},
    )
    handler.setFormatter(fmt)
    logger.addHandler(handler)
    return logger
