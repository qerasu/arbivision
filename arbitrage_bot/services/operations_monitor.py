from time import monotonic

from arbitrage_bot.services.system_notifier import send_system_notification

_DUPLICATE_WARNING_ROWS = 50
_DUPLICATE_CRITICAL_ROWS = 200
_DUPLICATE_STREAK_ROWS = 20
_DUPLICATE_STREAK_CYCLES = 5
_ORDERBOOK_WARNING_RATIO = 0.85
_ORDERBOOK_CRITICAL_RATIO = 0.70
_ORDERBOOK_STREAK_CYCLES = 3
_DELIVERABLE_WARNING_STREAK = 5
_DELIVERABLE_CRITICAL_STREAK = 10
_TELEGRAM_DOWN_THRESHOLD_SECONDS = 180.0
_TELEGRAM_RECOVERY_SECONDS = 60.0
_TELEGRAM_IGNORED_FAILURE_MARKERS = (
    "request timeout error",
    "clientoserror:",
    "record layer failure",
)

_duplicate_state = {}
_orderbook_state = {"warning_streak": 0, "critical_streak": 0, "severity": None}
_deliverable_state = {"streak": 0, "severity": None}
_telegram_state = {
    "first_failure_at": None,
    "last_failure_at": None,
    "active": False,
    "outage_detected": False,
}


def reset_monitor_state():
    _duplicate_state.clear()
    _orderbook_state["warning_streak"] = 0
    _orderbook_state["critical_streak"] = 0
    _orderbook_state["severity"] = None
    _deliverable_state["streak"] = 0
    _deliverable_state["severity"] = None
    _telegram_state["first_failure_at"] = None
    _telegram_state["last_failure_at"] = None
    _telegram_state["active"] = False
    _telegram_state["outage_detected"] = False


async def record_duplicate_markets(source, duplicate_rows):
    normalized_source = str(source or "unknown")
    state = _duplicate_state.setdefault(
        normalized_source,
        {
            "streak": 0,
            "severity": None,
        },
    )
    rows = int(duplicate_rows or 0)
    if rows >= _DUPLICATE_STREAK_ROWS:
        state["streak"] += 1
    else:
        state["streak"] = 0

    desired_severity = None
    if rows >= _DUPLICATE_CRITICAL_ROWS:
        desired_severity = "critical"
        details = f"duplicate markets spike on {normalized_source}: {rows} rows removed in one sync"
    elif rows >= _DUPLICATE_WARNING_ROWS:
        desired_severity = "warning"
        details = f"duplicate markets elevated on {normalized_source}: {rows} rows removed in one sync"
    elif state["streak"] >= _DUPLICATE_STREAK_CYCLES:
        desired_severity = "warning"
        details = (
            f"duplicate markets keep repeating on {normalized_source}: "
            f"{rows} rows removed, streak={state['streak']} syncs"
        )
    else:
        details = None

    if desired_severity is None:
        if state["severity"] is not None:
            await send_system_notification(
                "monitor",
                f"duplicate markets {normalized_source}",
                f"duplicate market anomaly recovered on {normalized_source}",
                level="recovery",
            )
            state["severity"] = None
        return

    if state["severity"] != desired_severity:
        await send_system_notification(
            "monitor",
            f"duplicate markets {normalized_source}",
            details,
            level=desired_severity,
        )
        state["severity"] = desired_severity


async def record_worker_cycle(active_pairs, pairs_with_books, opportunities, deliverable_opportunities):
    await _record_orderbook_coverage(active_pairs, pairs_with_books)
    await _record_deliverable_stall(opportunities, deliverable_opportunities)
    await evaluate_telegram_connectivity()


def record_telegram_polling_failure(message):
    text = str(message or "")
    if "Failed to fetch updates -" not in text:
        return
    lowered = text.lower()
    if any(marker in lowered for marker in _TELEGRAM_IGNORED_FAILURE_MARKERS):
        return

    now = monotonic()
    if _telegram_state["first_failure_at"] is None:
        _telegram_state["first_failure_at"] = now
    _telegram_state["last_failure_at"] = now


async def evaluate_telegram_connectivity(now=None):
    current_time = monotonic() if now is None else float(now)
    first_failure_at = _telegram_state["first_failure_at"]
    last_failure_at = _telegram_state["last_failure_at"]

    if first_failure_at is None or last_failure_at is None:
        return

    if not _telegram_state["outage_detected"]:
        if current_time - first_failure_at >= _TELEGRAM_DOWN_THRESHOLD_SECONDS:
            _telegram_state["outage_detected"] = True
            warning_sent = await send_system_notification(
                "monitor",
                "telegram polling",
                "telegram polling appears down for more than 3 minutes",
                level="warning",
            )
            if warning_sent:
                _telegram_state["active"] = True
        elif current_time - last_failure_at >= _TELEGRAM_RECOVERY_SECONDS:
            _telegram_state["first_failure_at"] = None
            _telegram_state["last_failure_at"] = None
        return

    if current_time - last_failure_at >= _TELEGRAM_RECOVERY_SECONDS:
        recovery_sent = await send_system_notification(
            "monitor",
            "telegram polling",
            "telegram polling connectivity recovered",
            level="recovery",
        )
        if recovery_sent:
            _telegram_state["first_failure_at"] = None
            _telegram_state["last_failure_at"] = None
            _telegram_state["active"] = False
            _telegram_state["outage_detected"] = False


async def _record_orderbook_coverage(active_pairs, pairs_with_books):
    active = int(active_pairs or 0)
    with_books = int(pairs_with_books or 0)
    if active <= 0:
        _orderbook_state["warning_streak"] = 0
        _orderbook_state["critical_streak"] = 0
        return

    ratio = with_books / active
    desired_severity = None
    details = None

    if ratio < _ORDERBOOK_CRITICAL_RATIO:
        _orderbook_state["critical_streak"] += 1
        _orderbook_state["warning_streak"] += 1
        if _orderbook_state["critical_streak"] >= _ORDERBOOK_STREAK_CYCLES:
            desired_severity = "critical"
            details = f"orderbook coverage degraded to {ratio:.1%} ({with_books}/{active})"
    elif ratio < _ORDERBOOK_WARNING_RATIO:
        _orderbook_state["warning_streak"] += 1
        _orderbook_state["critical_streak"] = 0
        if _orderbook_state["warning_streak"] >= _ORDERBOOK_STREAK_CYCLES:
            desired_severity = "warning"
            details = f"orderbook coverage dropped to {ratio:.1%} ({with_books}/{active})"
    else:
        _orderbook_state["warning_streak"] = 0
        _orderbook_state["critical_streak"] = 0

    if desired_severity is None:
        if _orderbook_state["severity"] is not None and ratio >= _ORDERBOOK_WARNING_RATIO:
            await send_system_notification(
                "monitor",
                "orderbook coverage",
                f"orderbook coverage recovered to {ratio:.1%} ({with_books}/{active})",
                level="recovery",
            )
            _orderbook_state["severity"] = None
        return

    if _orderbook_state["severity"] != desired_severity:
        await send_system_notification(
            "monitor",
            "orderbook coverage",
            details,
            level=desired_severity,
        )
        _orderbook_state["severity"] = desired_severity


async def _record_deliverable_stall(opportunities, deliverable_opportunities):
    has_stall = int(opportunities or 0) > 0 and int(deliverable_opportunities or 0) == 0
    if has_stall:
        _deliverable_state["streak"] += 1
    else:
        if _deliverable_state["severity"] is not None:
            await send_system_notification(
                "monitor",
                "deliverable opportunities",
                "deliverable opportunities recovered",
                level="recovery",
            )
        _deliverable_state["streak"] = 0
        _deliverable_state["severity"] = None
        return

    desired_severity = None
    if _deliverable_state["streak"] >= _DELIVERABLE_CRITICAL_STREAK:
        desired_severity = "critical"
    elif _deliverable_state["streak"] >= _DELIVERABLE_WARNING_STREAK:
        desired_severity = "warning"

    if desired_severity is None:
        return

    if _deliverable_state["severity"] != desired_severity:
        await send_system_notification(
            "monitor",
            "deliverable opportunities",
            (
                "opportunities are being found but none are deliverable: "
                f"opportunities={int(opportunities or 0)}, "
                f"deliverable={int(deliverable_opportunities or 0)}, "
                f"streak={_deliverable_state['streak']}"
            ),
            level=desired_severity,
        )
        _deliverable_state["severity"] = desired_severity