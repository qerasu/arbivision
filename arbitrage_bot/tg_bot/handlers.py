from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message


router = Router()


@router.message(Command("start"))
async def cmd_start(message):
    await message.answer("Hello! I'm arbitrage alert bot. Use /status to check status.")


@router.message(Command("status"))
async def cmd_status(message):
    await message.answer("Status: online. Market updates running in background.")