#!/usr/bin/env python3
# utils.py v2.0.0 (11.03.2026)
# - Актуализирована версия модуля

import re
import logging
from typing import Optional
import aiohttp
from config import TONCENTER_API_KEY, TON_WALLET

__version__ = '2.0.0'

logger = logging.getLogger(__name__)


def validate_txid(txid: str) -> bool:
    """
    Проверяет формат TON TXID (64 hex символа).
    ВНИМАНИЕ: это только базовая проверка формата.
    Для реальной валидации используйте verify_ton_transaction.
    """
    return bool(re.fullmatch(r'[0-9a-fA-F]{64}', txid))


async def verify_ton_transaction(txid: str, expected_amount: float) -> bool:
    """Проверяет транзакцию TON через API toncenter.com"""
    if not TONCENTER_API_KEY:
        logger.error("TONCENTER_API_KEY не задан")
        return False
    url = "https://toncenter.com/api/v2/getTransaction"
    params = {
        "hash": txid,
        "api_key": TONCENTER_API_KEY
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as resp:
                if resp.status != 200:
                    logger.error(f"TON API вернул статус {resp.status}")
                    return False
                data = await resp.json()
        if not data.get("ok"):
            logger.error(f"TON API ответил ошибкой: {data}")
            return False
        tx = data["result"]
        # Проверяем, что транзакция исходящая на наш кошелёк
        if not tx.get("out_msgs"):
            logger.warning("Нет исходящих сообщений в транзакции")
            return False
        out_msg = tx["out_msgs"][0]
        if out_msg["destination"]["address"] != TON_WALLET:
            logger.warning(f"Адрес получателя не совпадает: {out_msg['destination']['address']} != {TON_WALLET}")
            return False
        # Сумма в нанотонах (1 TON = 1e9)
        amount_nano = int(out_msg["value"])
        amount_ton = amount_nano / 1_000_000_000
        # Допустимая погрешность 0.01 TON
        return abs(amount_ton - expected_amount) < 0.01
    except Exception as e:
        logger.error(f"Ошибка проверки TON транзакции: {e}")
        return False


def truncate_text(text: str, max_length: int = 1000) -> str:
    """Обрезает текст до максимальной длины, добавляя многоточие"""
    if len(text) <= max_length:
        return text
    return text[:max_length - 3] + "..."


def escape_markdown(text: str) -> str:
    """
    Экранирует специальные символы для Telegram MarkdownV2.
    Используется для защиты от ошибок форматирования.
    """
    if not text:
        return ''
    special_chars = ['_', '*', '[', ']', '(', ')', '~', '`', '>', '#', '+', '-', '=', '|', '{', '}', '.', '!']
    for char in special_chars:
        text = text.replace(char, f'\\{char}')
    return text


def safe_md(text: str) -> str:
    """Безопасное экранирование для Markdown (псевдоним)"""
    return escape_markdown(str(text) if text else '')


async def check_user_exists(bot, user_id: int) -> bool:
    """Проверяет, существует ли пользователь в Telegram"""
    try:
        await bot.get_chat(user_id)
        return True
    except Exception:
        return False


async def shutdown(app):
    """Graceful shutdown: закрываем соединения с БД и останавливаем задачи"""
    logger.info("Получен сигнал завершения, закрываем соединения...")
    from database import Database
    await Database.close()
    # Остановка фоновых задач
    for task in app.bot_data.get("background_tasks", []):
        task.cancel()
    logger.info("Shutdown completed")