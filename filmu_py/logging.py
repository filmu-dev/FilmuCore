"""Structured logging setup for filmu-python."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any

import structlog

from .config import Settings
from .core.log_stream import LogStreamBroker

_stream_handler: logging.Handler | None = None
_DEFAULT_LOG_RECORD_FIELDS = frozenset(logging.makeLogRecord({}).__dict__.keys())


def _optional_string(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _extract_log_record_extra(record: logging.LogRecord) -> dict[str, Any]:
    return {
        key: value
        for key, value in record.__dict__.items()
        if key not in _DEFAULT_LOG_RECORD_FIELDS and key not in {"message", "asctime"}
    }


class LogStreamHandler(logging.Handler):
    """Stdlib logging handler that feeds the in-memory log stream broker."""

    def __init__(self, log_stream: LogStreamBroker) -> None:
        super().__init__()
        self._log_stream = log_stream

    def emit(self, record: logging.LogRecord) -> None:
        """Append one log record to history and live subscribers."""

        try:
            extra = _extract_log_record_extra(record)
            stage = _optional_string(extra.pop("stage", None)) or _optional_string(
                extra.pop("worker_stage", None)
            )
            self._log_stream.record(
                level=record.levelname,
                message=record.getMessage(),
                timestamp=datetime.fromtimestamp(record.created, UTC).isoformat(),
                event=str(record.msg) if isinstance(record.msg, str) else record.getMessage(),
                logger=record.name,
                worker_id=_optional_string(extra.pop("worker_id", None)),
                item_id=_optional_string(extra.pop("item_id", None)),
                stage=stage,
                extra=extra,
            )
        except Exception:
            self.handleError(record)


def configure_logging(settings: Settings) -> None:
    """Configure stdlib and structlog."""

    level = getattr(logging, settings.log_level.upper(), logging.INFO)

    logging.basicConfig(level=level, format="%(message)s")

    if settings.env == "development" or level > logging.DEBUG:
        logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(level),
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def attach_log_stream(log_stream: LogStreamBroker) -> None:
    """Attach a single live/history log stream handler to the root logger."""

    global _stream_handler

    root_logger = logging.getLogger()
    if _stream_handler is not None:
        root_logger.removeHandler(_stream_handler)
        _stream_handler.close()

    _stream_handler = LogStreamHandler(log_stream)
    root_logger.addHandler(_stream_handler)


def detach_log_stream() -> None:
    """Detach the live/history log stream handler when the app shuts down."""

    global _stream_handler

    if _stream_handler is None:
        return

    root_logger = logging.getLogger()
    root_logger.removeHandler(_stream_handler)
    _stream_handler.close()
    _stream_handler = None
