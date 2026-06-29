import asyncio
import hashlib
import traceback
from time import monotonic

from aiogram import Bot

from arbitrage_bot.core.config import settings
from arbitrage_bot.core.logging import get_logger
from arbitrage_bot.core.redis import get_redis

_last_sent_at = {}
_MAX_DEDUPE_ENTRIES = 500
_MAX_ERROR_DETAILS_LENGTH = 280
_shared_bot = None
log = get_logger("system_notifier")
_TRANSIENT_NETWORK_MARKERS = (
    "record layer failure",
    "request timeout error",
    "timed out",
    "timeout",
    "temporary failure in name resolution",
    "name or service not known",
    "nodename nor servname provided",
    "connection reset by peer",
    "server disconnected",
    "clientoserror",
    "connecterror",
    "readtimeout",
    "connecttimeout",
)


def _get_system_error_chat_ids():
    if settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS:
        return settings.TELEGRAM_SYSTEM_ERROR_CHAT_IDS
    return settings.TELEGRAM_DEFAULT_CHAT_IDS


def _format_system_error_message(source, operation, error):
    details = format_error_details(error)

    return (
        "system error\n"
        f"source: {source}\n"
        f"operation: {operation}\n"
        f"type: {type(error).__name__}\n"
        f"details: {details}"
    )


def format_error_details(error):
    details = _extract_error_details(error)
    noise_marker = "For more information check:"

    if noise_marker in details:
        details = details.split(noise_marker, 1)[0].rstrip()

    for marker in ("[SQL:", "[parameters:", "(Background on this error at:"):
        if marker in details:
            details = details.split(marker, 1)[0].rstrip()

    response = getattr(error, "response", None)
    request = getattr(response, "request", None) if response is not None else None
    status_code = getattr(response, "status_code", None)
    method = getattr(request, "method", None)
    url = getattr(request, "url", None)

    if status_code is not None and method and url:
        details = f"{method} {url} -> HTTP {status_code}: {details}"
    elif status_code is not None:
        details = f"HTTP {status_code}: {details}"

    details = " ".join(details.split())

    if len(details) > _MAX_ERROR_DETAILS_LENGTH:
        details = f"{details[:_MAX_ERROR_DETAILS_LENGTH - 3]}..."

    return details or repr(error)


def format_compact_error(error):
    return f"{type(error).__name__}: {format_error_details(error)}"


def is_transient_network_error(error):
    details = format_error_details(error).lower()
    return any(marker in details for marker in _TRANSIENT_NETWORK_MARKERS)


def _extract_error_details(error):
    orig = getattr(error, "orig", None)
    if orig is not None:
        orig_details = str(orig or "").strip()
        if orig_details:
            return orig_details

    details = str(error or "").strip()
    if details:
        return details

    tb = getattr(error, "__traceback__", None)
    if tb is not None:
        frames = traceback.extract_tb(tb)
        if frames:
            frame = frames[-1]
            return f"raised at {frame.filename}:{frame.lineno} in {frame.name}"

    return ""


def _should_skip_notification(dedupe_key):
    cooldown = settings.TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS
    if cooldown <= 0:
        return False

    now = monotonic()
    last_sent_at = _last_sent_at.get(dedupe_key)
    if last_sent_at is not None and now - last_sent_at < cooldown:
        return True

    _last_sent_at[dedupe_key] = now

    # discard expired entries before sacrificing valid dedupe keys
    if len(_last_sent_at) > _MAX_DEDUPE_ENTRIES:
        expired = [k for k, t in _last_sent_at.items() if now - t >= cooldown]
        for k in expired:
            _last_sent_at.pop(k, None)
        if len(_last_sent_at) > _MAX_DEDUPE_ENTRIES:
            sorted_keys = sorted(_last_sent_at, key=_last_sent_at.get)
            for k in sorted_keys[:len(sorted_keys) // 2]:
                _last_sent_at.pop(k, None)
    return False


def _system_error_redis_key(dedupe_key):
    digest = hashlib.sha256(dedupe_key.encode("utf-8")).hexdigest()
    return f"system-error-dedupe:{digest}"


async def _should_skip_notification_async(dedupe_key):
    cooldown = settings.TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS
    if cooldown <= 0:
        return False

    try:
        redis = get_redis()
        if redis is not None:
            redis_key = _system_error_redis_key(dedupe_key)
            created = await redis.set(redis_key, "1", ex=max(1, int(cooldown)), nx=True)
            if created:
                return False
            return True
    except Exception:
        pass

    return _should_skip_notification(dedupe_key)


def _get_shared_bot():
    global _shared_bot
    token = settings.TELEGRAM_BOT_TOKEN
    if not token:
        return None
    if _shared_bot is None:
        _shared_bot = Bot(token=token)
    return _shared_bot


async def close_shared_bot():
    global _shared_bot
    if _shared_bot is not None:
        await _shared_bot.session.close()
        _shared_bot = None


async def send_system_error_notification(source, operation, error):
    bot = _get_shared_bot()
    chat_ids = _get_system_error_chat_ids()

    if not bot or not chat_ids:
        return False

    details = format_error_details(error)
    dedupe_key = f"{source}:{operation}:{type(error).__name__}:{details}"
    
    if await _should_skip_notification_async(dedupe_key):
        return False

    message = _format_system_error_message(source, operation, error)
    try:
        for chat_id in chat_ids:
            await bot.send_message(chat_id=chat_id, text=message)
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        log.error("failed to send system error notification", error=str(exc))
        return False
