import os
from pathlib import Path

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


def _get_bool_setting(name, default):
    value = os.getenv(name, str(default)).strip().lower()

    if value in {"1", "true", "yes", "on", "y", "t"}:
        return True

    if value in {"0", "false", "no", "off", "n", "f"}:
        return False

    return default


# for telegram chat ids
def _get_list_setting(name, default):
    raw = os.getenv(name, "")

    if not raw:
        return default
        
    return [x.strip() for x in raw.split(",") if x.strip()]


class Settings:
    POSTGRES_USER = os.getenv("POSTGRES_USER", "arb_user")
    POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "arb_pass")
    POSTGRES_DB = os.getenv("POSTGRES_DB", "arbitrage_db")
    POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
    POSTGRES_PORT = _get_int_setting("POSTGRES_PORT", 5432)

    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = _get_int_setting("REDIS_PORT", 6379)
    REDIS_DB = _get_int_setting("REDIS_DB", 0)

    POLYMARKET_ENABLED = _get_bool_setting("POLYMARKET_ENABLED", True)
    PREDICT_FUN_ENABLED = _get_bool_setting("PREDICT_FUN_ENABLED", True)
    PREDICT_FUN_API_KEY = os.getenv("PREDICT_FUN_API_KEY", "")

    FEE_POLYMARKET_BPS = _get_float_setting("FEE_POLYMARKET_BPS", 180.0) # 180.0 max
    FEE_PREDICT_FUN_BPS = _get_float_setting("FEE_PREDICT_FUN_BPS", 200.0) # 200.0 max
    ALERTS_DEDUPE_TTL_SECONDS = _get_int_setting("ALERTS_DEDUPE_TTL_SECONDS", 600)
    ALERTS_DELTA_PROFIT_THRESHOLD_USD = _get_float_setting("ALERTS_DELTA_PROFIT_THRESHOLD_USD", 3.0)
    ALERTS_DELTA_ROI_THRESHOLD_PERCENT = _get_float_setting("ALERTS_DELTA_ROI_THRESHOLD_PERCENT", 0.5)
    MAX_MARKET_PAIRS_PER_LOOP = _get_int_setting("MAX_MARKET_PAIRS_PER_LOOP", 0)
    EMPTY_ORDERBOOK_THRESHOLD = _get_int_setting("EMPTY_ORDERBOOK_THRESHOLD", 3)
    ORDERBOOK_CACHE_TTL_SECONDS = _get_float_setting("ORDERBOOK_CACHE_TTL_SECONDS", 2.0)
    ORDERBOOK_CACHE_MAX_ITEMS = _get_int_setting("ORDERBOOK_CACHE_MAX_ITEMS", 5000)
    ORDERBOOK_POLYMARKET_BATCH_SIZE = _get_int_setting("ORDERBOOK_POLYMARKET_BATCH_SIZE", 100)
    ORDERBOOK_PREDICT_FUN_CONCURRENCY = _get_int_setting("ORDERBOOK_PREDICT_FUN_CONCURRENCY", 8)

    MARKET_REFRESH_SECONDS = _get_int_setting("MARKET_REFRESH_SECONDS", 60)

    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_DEFAULT_CHAT_IDS = _get_list_setting("TELEGRAM_DEFAULT_CHAT_IDS", [])
    TELEGRAM_SYSTEM_ERROR_CHAT_IDS = _get_list_setting("TELEGRAM_SYSTEM_ERROR_CHAT_IDS", [])
    TELEGRAM_ALERTS_POLL_SECONDS = _get_float_setting("TELEGRAM_ALERTS_POLL_SECONDS", 0.5)
    FANOUT_TARGET_CACHE_TTL_SECONDS = _get_float_setting("FANOUT_TARGET_CACHE_TTL_SECONDS", 5.0)
    TELEGRAM_DELIVERY_RETRY_SECONDS = _get_float_setting("TELEGRAM_DELIVERY_RETRY_SECONDS", 15.0)
    TELEGRAM_DELIVERY_MAX_ATTEMPTS = _get_int_setting("TELEGRAM_DELIVERY_MAX_ATTEMPTS", 3)
    TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS = _get_float_setting("TELEGRAM_SYSTEM_ERROR_COOLDOWN_SECONDS", 300.0)
    ADMIN_API_TOKEN = os.getenv("ADMIN_API_TOKEN", "")
    APP_RUNTIME_MODE = os.getenv("APP_RUNTIME_MODE", "all").strip().lower()

    @property
    def database_url(self):
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"


    @property
    def redis_url(self):
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


settings = Settings()