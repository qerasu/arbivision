from collections import Counter
from datetime import datetime
from datetime import timezone

_counters = Counter()
_started_at = datetime.now(timezone.utc)


def get_started_at():
    return _started_at


def incr_counter(name, amount=1):
    _counters[str(name)] += int(amount)


def snapshot_counters():
    return dict(_counters)


def reset_counters():
    _counters.clear()
