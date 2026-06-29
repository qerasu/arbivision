import os
from pathlib import Path
from urllib.parse import quote_plus

from arbitrage_bot.core.env_loader import load_env_file

ENV_FILE_PATH = Path.home() / ".config" / "arbivision" / ".env"


load_env_file(os.path.expanduser(str(ENV_FILE_PATH)))


def _get_int_setting(name, default):
    try:
        return int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _get_float_setting(name, default):
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _get_set_setting(name, default):
    raw = os.getenv(name, "")

    if not raw:
        return frozenset(default)

    return frozenset(x.strip() for x in raw.split(",") if x.strip())


class Settings:

    def __init__(self):
        self.POSTGRES_USER = os.getenv("POSTGRES_USER", "arb_user")
        self.POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "arb_pass")
        self.POSTGRES_DB = os.getenv("POSTGRES_DB", "arbitrage_db")
        self.POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
        self.POSTGRES_PORT = _get_int_setting("POSTGRES_PORT", 5432)

        self.REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
        self.REDIS_PORT = _get_int_setting("REDIS_PORT", 6379)
        self.REDIS_DB = _get_int_setting("REDIS_DB", 0)

        self.PREDICT_FUN_API_KEY = os.getenv("PREDICT_FUN_API_KEY", "")

        self.FEE_POLYMARKET_BPS = _get_float_setting("FEE_POLYMARKET_BPS", 90.0)
        self.FEE_PREDICT_FUN_BPS = _get_float_setting("FEE_PREDICT_FUN_BPS", 100.0)
        self.ALERTS_DEDUPE_TTL_SECONDS = _get_int_setting("ALERTS_DEDUPE_TTL_SECONDS", 600)
        self.ALERTS_DELTA_PROFIT_THRESHOLD_USD = _get_float_setting("ALERTS_DELTA_PROFIT_THRESHOLD_USD", 3.0)
        self.ALERTS_DELTA_ROI_THRESHOLD_PERCENT = _get_float_setting("ALERTS_DELTA_ROI_THRESHOLD_PERCENT", 0.5)
        self.MAX_MARKET_PAIRS_PER_LOOP = _get_int_setting("MAX_MARKET_PAIRS_PER_LOOP", 0)
        self.HOT_PAIR_QUEUE_MAX_SIZE = _get_int_setting("HOT_PAIR_QUEUE_MAX_SIZE", 1000)
        self.EMPTY_ORDERBOOK_THRESHOLD = _get_int_setting("EMPTY_ORDERBOOK_THRESHOLD", 3)
        self.ORDERBOOK_CACHE_TTL_SECONDS = _get_float_setting("ORDERBOOK_CACHE_TTL_SECONDS", 1.0)
        self.ORDERBOOK_CACHE_MAX_ITEMS = _get_int_setting("ORDERBOOK_CACHE_MAX_ITEMS", 5000)
        self.ORDERBOOK_POLYMARKET_BATCH_SIZE = _get_int_setting("ORDERBOOK_POLYMARKET_BATCH_SIZE", 100)
        self.ORDERBOOK_PREDICT_FUN_CONCURRENCY = _get_int_setting("ORDERBOOK_PREDICT_FUN_CONCURRENCY", 12)
        self.MAX_ACTIVE_PAIRS_PER_CYCLE = _get_int_setting("MAX_ACTIVE_PAIRS_PER_CYCLE", 450)

        self.MARKET_REFRESH_SECONDS = _get_int_setting("MARKET_REFRESH_SECONDS", 5)
        self.MARKET_SYNC_INTERVAL_SECONDS = _get_float_setting("MARKET_SYNC_INTERVAL_SECONDS", 60.0)
        self.POLYMARKET_INCREMENTAL_MAX_PAGES = _get_int_setting("POLYMARKET_INCREMENTAL_MAX_PAGES", 20)
        self.POLYMARKET_FULL_SYNC_INTERVAL_SECONDS = _get_float_setting("POLYMARKET_FULL_SYNC_INTERVAL_SECONDS", 1800.0)
        self.MATCHER_FULL_REMATCH_INTERVAL_SECONDS = _get_float_setting("MATCHER_FULL_REMATCH_INTERVAL_SECONDS", 1800.0)
        self.DB_CLEANUP_INTERVAL_SECONDS = _get_float_setting("DB_CLEANUP_INTERVAL_SECONDS", 10800.0)
        self.DB_CLEANUP_RETENTION_SECONDS = _get_float_setting("DB_CLEANUP_RETENTION_SECONDS", 21600.0)

        self.TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
        self.TELEGRAM_DEFAULT_CHAT_IDS = _get_set_setting("TELEGRAM_DEFAULT_CHAT_IDS", [])
        self.TELEGRAM_SYSTEM_ERROR_CHAT_IDS = _get_set_setting("TELEGRAM_SYSTEM_ERROR_CHAT_IDS", [])
        self.FANOUT_TARGET_CACHE_TTL_SECONDS = _get_float_setting("FANOUT_TARGET_CACHE_TTL_SECONDS", 2.0)
        self.TELEGRAM_SEND_CONCURRENCY = _get_int_setting("TELEGRAM_SEND_CONCURRENCY", 8)
        self.TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS = _get_float_setting("TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS", 300.0)
        self.APP_RUNTIME_MODE = os.getenv("APP_RUNTIME_MODE", "all").strip().lower()


    @property
    def database_url(self):
        return f"postgresql+asyncpg://{quote_plus(self.POSTGRES_USER)}:{quote_plus(self.POSTGRES_PASSWORD)}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"


    @property
    def redis_url(self):
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


settings = Settings()
