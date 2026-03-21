import asyncio
from time import monotonic

from aiogram import Bot

from arbitrage_bot.core.config import settings

_last_sent_at = {}
_MAX_ERROR_DETAILS_LENGTH = 280


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


def _extract_error_details(error):
    orig = getattr(error, "orig", None)
    if orig is not None:
        orig_details = str(orig or "").strip()
        if orig_details:
            return orig_details

    return str(error or "").strip()


def _should_skip_notification(dedupe_key):
    cooldown = settings.TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS
    if cooldown <= 0:
        return False

    now = monotonic()
    last_sent_at = _last_sent_at.get(dedupe_key)
    if last_sent_at is not None and now - last_sent_at < cooldown:
        return True

    _last_sent_at[dedupe_key] = now
    return False


async def send_system_error_notification(source, operation, error):
    token = settings.TELEGRAM_BOT_TOKEN
    chat_ids = _get_system_error_chat_ids()
    if not token or not chat_ids:
        return False

    details = format_error_details(error)
    dedupe_key = f"{source}:{operation}:{type(error).__name__}:{details}"
    if _should_skip_notification(dedupe_key):
        return False

    message = _format_system_error_message(source, operation, error)
    bot = Bot(token=token)
    try:
        for chat_id in chat_ids:
            await bot.send_message(chat_id=chat_id, text=message)
        return True
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(f"failed to send telegram system error notification: {exc}")
        return False
    finally:
        await bot.session.close()
