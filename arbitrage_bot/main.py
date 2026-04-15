from contextlib import asynccontextmanager
from fastapi import FastAPI
from arbitrage_bot.api import internal
from arbitrage_bot.core.config import settings
from arbitrage_bot.runtime import managed_runtime
from arbitrage_bot.runtime import run_telegram_runtime
from arbitrage_bot.runtime import run_worker_runtime


@asynccontextmanager
async def lifespan(_app):
    coroutines = []

    if settings.APP_RUNTIME_MODE in {"all", "worker"}:
        coroutines.append(run_worker_runtime())

    if settings.APP_RUNTIME_MODE in {"all", "telegram"}:
        coroutines.append(run_telegram_runtime())

    async with managed_runtime(*coroutines):
        yield

app = FastAPI(title="Arbitrage Alert Bot API", lifespan=lifespan)


@app.get("/")
async def root():
    return {
        "message": "Arbitrage Alert Bot API is running",
        "docs": "/docs",
        "health": "/api/health",
        "status": "/api/status",
    }

app.include_router(internal.router, prefix="/api")
