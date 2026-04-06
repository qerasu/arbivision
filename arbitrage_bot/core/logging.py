import logging
import os

import structlog

_LEVEL_NAMES = {
    "critical": logging.CRITICAL,
    "error": logging.ERROR,
    "warning": logging.WARNING,
    "info": logging.INFO,
    "debug": logging.DEBUG,
}


class _SuppressTelegramPollingTimeoutFilter(logging.Filter):
    def filter(self, record):
        message = record.getMessage()
        if "Failed to fetch updates - TelegramNetworkError: HTTP Client says - Request timeout error" in message:
            return False
        if message.startswith("Sleep for ") and "bot id =" in message:
            return False
        if message.startswith("Connection established (tryings ="):
            return False
        return True


def _resolve_log_level():
    raw = os.getenv("LOG_LEVEL", "info").strip().lower()
    return _LEVEL_NAMES.get(raw, logging.INFO)


_log_level = _resolve_log_level()
logging.basicConfig(level=_log_level, format="%(message)s")
logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
logging.getLogger("aiogram").setLevel(logging.ERROR)
_aiogram_dispatcher_logger = logging.getLogger("aiogram.dispatcher")
_aiogram_dispatcher_logger.setLevel(logging.WARNING)
_aiogram_dispatcher_logger.addFilter(_SuppressTelegramPollingTimeoutFilter())
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(pad_level=False),
    ],
    logger_factory=structlog.PrintLoggerFactory(),
    wrapper_class=structlog.make_filtering_bound_logger(_log_level),
    cache_logger_on_first_use=True,
)


def get_logger(name=None):
    if name:
        return structlog.get_logger(component=name)
        
    return structlog.get_logger()