import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
from arbitrage_bot.api import internal
from arbitrage_bot.worker import run_sync_loop
from arbitrage_bot.tg_bot.bot import start_polling
from arbitrage_bot.services.system_notifier import close_shared_bot


@asynccontextmanager
async def lifespan(_app):
    sync_task = asyncio.create_task(run_sync_loop())
    bot_task = asyncio.create_task(start_polling())

    try:
        yield
    finally:
        sync_task.cancel()
        bot_task.cancel()
        await asyncio.gather(sync_task, bot_task, return_exceptions=True)
        await close_shared_bot()

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