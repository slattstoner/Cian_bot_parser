import re
import logging
from typing import Optional

logger = logging.getLogger(__name__)


def validate_txid(txid: str) -> bool:
    """
    Проверяет формат TON TXID (64 hex символа).
    ВНИМАНИЕ: это только базовая проверка формата.
    Для реальной валидации необходимо обращаться к TON API.
    """
    return bool(re.fullmatch(r'[0-9a-fA-F]{64}', txid))


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
