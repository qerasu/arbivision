from collections import Counter
from threading import Lock

_counters = Counter()
_lock = Lock()


def incr_counter(name, amount=1):
    with _lock:
        _counters[str(name)] += int(amount)


def snapshot_counters():
    with _lock:
        return dict(_counters)


def snapshot_and_reset_counters():
    with _lock:
        snapshot = dict(_counters)
        _counters.clear()
        return snapshot


def reset_counters():
    with _lock:
        _counters.clear()