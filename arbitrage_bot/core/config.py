import os
from pathlib import Path

ENV_FILE_PATH = Path.home() / ".config" / "arbivision" / ".env"


def _load_env_file(path):
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                # ignore malformed lines in env file to avoid startup failures
                continue

            key, val = line.split("=", 1)
            key = key.strip().removeprefix("export ").strip()
            val = val.strip()

            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]

            if key:
                os.environ[key] = val


_load_env_file(os.path.expanduser(str(ENV_FILE_PATH)))


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

    FEE_POLYMARKET_BPS = _get_float_setting("FEE_POLYMARKET_BPS", 0.0)
    FEE_PREDICT_FUN_BPS = _get_float_setting("FEE_PREDICT_FUN_BPS", 0.0)
    MIN_PROFIT_USD = _get_float_setting("MIN_PROFIT_USD", 5.0)
    MIN_ROI_PERCENT = _get_float_setting("MIN_ROI_PERCENT", 1.5)
    ALERTS_DEDUPE_TTL_SECONDS = _get_int_setting("ALERTS_DEDUPE_TTL_SECONDS", 600)
    ALERTS_DELTA_PROFIT_THRESHOLD_USD = _get_float_setting("ALERTS_DELTA_PROFIT_THRESHOLD_USD", 3.0)
    ALERTS_DELTA_ROI_THRESHOLD_PERCENT = _get_float_setting("ALERTS_DELTA_ROI_THRESHOLD_PERCENT", 0.5)
    MAX_MARKET_PAIRS_PER_LOOP = _get_int_setting("MAX_MARKET_PAIRS_PER_LOOP", 200)
    
    MARKET_REFRESH_SECONDS = _get_int_setting("MARKET_REFRESH_SECONDS", 15)
    FALLBACK_ORDERBOOK_POLL_SECONDS = _get_int_setting("FALLBACK_ORDERBOOK_POLL_SECONDS", 5)
    
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
    TELEGRAM_DEFAULT_CHAT_IDS = _get_list_setting("TELEGRAM_DEFAULT_CHAT_IDS", [])
    TELEGRAM_ALERTS_POLL_SECONDS = _get_float_setting("TELEGRAM_ALERTS_POLL_SECONDS", 2.0)
    ADMIN_API_TOKEN = os.getenv("ADMIN_API_TOKEN", "")


    @property
    def database_url(self):
        return f"postgresql+asyncpg://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"


    @property
    def redis_url(self):
        return f"redis://{self.REDIS_HOST}:{self.REDIS_PORT}/{self.REDIS_DB}"


settings = Settings()
