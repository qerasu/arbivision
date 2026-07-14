import os
import unittest
from pathlib import Path

from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from arbitrage_bot.core.config import settings
from arbitrage_bot.models.orm import Market

ENV_FILE_PATH = Path.home() / ".config" / "arbivision" / ".env"


def _load_env_file(path):
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, val = line.split("=", 1)
            key = key.strip().removeprefix("export ").strip()
            val = val.strip()
            if not key:
                continue

            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]

            os.environ[key] = val


@unittest.skipUnless(
    os.environ.get("RUN_LIVE_DB_TESTS") == "1",
    "set RUN_LIVE_DB_TESTS=1 to run live database smoke tests",
)
class LiveDatabaseSmokeTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        _load_env_file(ENV_FILE_PATH)
        self.engine = create_async_engine(
            settings.database_url,
            echo=False,
            future=True,
        )
        self.session_factory = async_sessionmaker(
            bind=self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
            autoflush=False,
        )


    async def asyncTearDown(self):
        await self.engine.dispose()


    async def test_markets_table_contains_persisted_rows(self):
        async with self.session_factory() as session:
            total_markets = (
                await session.execute(select(func.count(Market.id)))
            ).scalar_one()

        self.assertGreater(
            total_markets,
            0,
            "expected at least one persisted market row in the live database",
        )


    async def test_latest_market_row_looks_like_real_persisted_data(self):
        async with self.session_factory() as session:
            latest_market = (
                await session.execute(
                    select(Market)
                    .order_by(Market.created_at.desc(), Market.id.desc())
                    .limit(1)
                )
            ).scalars().first()

        self.assertIsNotNone(latest_market, "expected at least one saved market row")
        self.assertTrue(latest_market.platform)
        self.assertTrue(latest_market.platform_market_id)
        self.assertTrue(latest_market.title)
        self.assertTrue(latest_market.normalized_title)
        self.assertIsInstance(latest_market.outcomes_json, list)
        self.assertIsInstance(latest_market.raw_payload_json, dict)
