import re
from telegram.ext import ContextTypes
from typing import Optional
import logging

logger = logging.getLogger(__name__)

def validate_txid(txid: str) -> bool:
    """Простая валидация TON TXID (64 hex символа)"""
    return bool(re.fullmatch(r'[0-9a-fA-F]{64}', txid))

def truncate_text(text: str, max_length: int = 1000) -> str:
    """Обрезает текст до максимальной длины, добавляя многоточие"""
    if len(text) <= max_length:
        return text
    return text[:max_length-3] + "..."

async def check_user_exists(bot, user_id: int) -> bool:
    """Проверяет, существует ли пользователь в Telegram"""
    try:
        await bot.get_chat(user_id)
        return True
    except Exception:
        return False

# Функция для безопасного получения имени пользователя
def get_user_mention(user):
    if user.username:
        return f"@{user.username}"
    return f"{user.full_name} (id: {user.id})"
    import logging
logger = logging.getLogger(__name__)

async def shutdown(app):
    """Graceful shutdown: закрываем соединения с БД и останавливаем задачи"""
    logger.info("Получен сигнал завершения, закрываем соединения...")
    from database import Database
    await Database.close()
    # Остановка других задач при необходимости