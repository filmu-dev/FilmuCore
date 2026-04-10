"""Structured logging setup for filmu-python."""

from __future__ import annotations

import contextlib
import logging
import logging.handlers
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson
import structlog
from structlog.contextvars import get_contextvars

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


def _current_trace_context() -> dict[str, str | None]:
    """Return active OpenTelemetry trace identifiers when a span exists."""

    try:
        from opentelemetry import trace

        span = trace.get_current_span()
        span_context = span.get_span_context()
    except Exception:
        return {"trace_id": None, "span_id": None}

    if span_context is None or not getattr(span_context, "is_valid", False):
        return {"trace_id": None, "span_id": None}

    return {
        "trace_id": f"{span_context.trace_id:032x}",
        "span_id": f"{span_context.span_id:016x}",
    }


def _structured_message_payload(message: str) -> dict[str, Any] | None:
    """Return parsed structlog JSON payload when the message already is structured JSON."""

    if not message.startswith("{"):
        return None
    try:
        payload = orjson.loads(message)
    except orjson.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


class CorrelationContextFilter(logging.Filter):
    """Attach correlation keys from structlog contextvars and tracing to log records."""

    def filter(self, record: logging.LogRecord) -> bool:
        context = get_contextvars()
        for key in ("request_id", "item_id", "worker_stage", "job_id", "worker_id", "plugin"):
            if not hasattr(record, key) and key in context:
                setattr(record, key, context[key])

        trace_context = _current_trace_context()
        if not hasattr(record, "trace_id") and trace_context["trace_id"] is not None:
            setattr(record, "trace_id", trace_context["trace_id"])
        if not hasattr(record, "span_id") and trace_context["span_id"] is not None:
            setattr(record, "span_id", trace_context["span_id"])
        return True


class StructuredJsonFormatter(logging.Formatter):
    """Emit file-backed NDJSON records for operator search and shipping."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created, UTC).isoformat()
        message = record.getMessage()
        payload = _structured_message_payload(message) or {}
        extra = _extract_log_record_extra(record)
        trace_context = _current_trace_context()

        structured: dict[str, Any] = {
            "@timestamp": timestamp,
            "log.level": record.levelname,
            "message": payload.get("event") if isinstance(payload.get("event"), str) else message,
            "event.original": message,
            "log.logger": record.name,
            "process.pid": record.process,
            "process.thread.name": record.threadName,
            "code.file.path": record.pathname,
            "code.function": record.funcName,
            "code.line": record.lineno,
            "request.id": extra.pop("request_id", None),
            "item.id": extra.pop("item_id", None),
            "worker.stage": extra.pop("worker_stage", None) or extra.pop("stage", None),
            "worker.job_id": extra.pop("job_id", None),
            "worker.id": extra.pop("worker_id", None),
            "plugin.name": extra.pop("plugin", None),
            "trace.id": extra.pop("trace_id", None) or trace_context["trace_id"],
            "span.id": extra.pop("span_id", None) or trace_context["span_id"],
        }

        for key, value in payload.items():
            if key in {"timestamp", "level", "event"}:
                continue
            structured[f"structlog.{key}"] = value

        if extra:
            structured["labels"] = extra

        return orjson.dumps(
            {key: value for key, value in structured.items() if value is not None}
        ).decode("utf-8")


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
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
        with contextlib.suppress(Exception):
            handler.close()

    correlation_filter = CorrelationContextFilter()

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(logging.Formatter("%(message)s"))
    console_handler.addFilter(correlation_filter)
    root_logger.addHandler(console_handler)

    if settings.logging.enabled:
        log_dir = Path(settings.logging.directory)
        if not log_dir.is_absolute():
            log_dir = Path.cwd() / log_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_dir / settings.logging.structured_filename,
            maxBytes=max(1, settings.logging.rotation_mb) * 1024 * 1024,
            backupCount=max(1, settings.logging.retention_files),
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(StructuredJsonFormatter())
        file_handler.addFilter(correlation_filter)
        root_logger.addHandler(file_handler)

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
    _stream_handler.addFilter(CorrelationContextFilter())
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
