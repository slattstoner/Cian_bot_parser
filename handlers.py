#!/usr/bin/env python3
# handlers.py v1.1 (04.03.2026)
# - Исправлена статистика (admin_stats_callback, stats)
# - Добавлена кнопка закрытия тикета для модераторов
# - Убрана реферальная комиссия, добавлен бонус 14 дней за приглашение
# - Интегрирована автоматическая проверка TON-транзакций через verify_ton_transaction
# - Улучшен формат объявлений (send_ad_to_user)
# - Атомарная отправка (ON CONFLICT) для предотвращения дублей
# - Обновлён профиль: отображение количества рефералов и полученных бонусов

import json
import logging
import asyncio
import time
import re
import csv
import io
from datetime import datetime
from typing import List, Optional

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, LabeledPrice
from telegram.ext import ContextTypes, ConversationHandler, CallbackQueryHandler, MessageHandler, filteres, Application 
from telegram.constants import ParseMode

from config import (
    ADMIN_ID, TON_WALLET, PAYMENT_PROVIDER_TOKEN, PRICES_RUB, PRICES_TON, PRICES_STARS,
    PLAN_DAYS, REFERRAL_BONUS_DAYS, PARSING_INTERVAL, TELEGRAM_RATE_LIMIT,
    DISTRICTS, ROOM_OPTIONS, DEAL_TYPE_NAMES, METRO_LINES, ALL_METRO_STATIONS,
    GITHUB_REPO, GITHUB_BRANCH, GITHUB_TOKEN, AUTO_UPDATE_CHECK_INTERVAL
)
from database import Database
from models import Ad
from parsers import fetch_all_ads
from utils import validate_txid, verify_ton_transaction, escape_markdown, check_user_exists

logger = logging.getLogger(__name__)

# ========== СОСТОЯНИЯ ConversationHandler ==========
ROLE_SELECTION = 0
SUPPORT_STATE = 1
METRO_SEARCH_STATE = 2
ADD_MOD_STATE = 3

# Глобальные словари для метро
STATION_TO_INDEX = {station: idx for idx, station in enumerate(ALL_METRO_STATIONS)}
INDEX_TO_STATION = {idx: station for station, idx in STATION_TO_INDEX.items()}


# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
async def notify_moderators(bot, text: str):
    """Уведомляет всех модераторов с правом view_tickets"""
    try:
        await bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode='Markdown')
    except Exception as e:
        logger.error(f"Ошибка уведомления админа: {e}")
    mods = await Database.get_moderators()
    for mod in mods:
        if await Database.has_permission(mod['user_id'], 'view_tickets'):
            try:
                await bot.send_message(chat_id=mod['user_id'], text=text, parse_mode='Markdown')
            except Exception:
                pass


def get_back_keyboard(callback: str = 'main_menu', text: str = '🏠 Главное меню'):
    return InlineKeyboardMarkup([[InlineKeyboardButton(text, callback_data=callback)]])


# ========== СТАРТ И МЕНЮ ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if await Database.is_banned(user_id):
        await update.message.reply_text("⛔ Вы забанены. Обратитесь в поддержку.")
        return ConversationHandler.END

    await Database.create_user(user_id)

    args = context.args
    if args and args[0].startswith('ref_'):
        try:
            referrer_id = int(args[0].split('_')[1])
            if referrer_id != user_id:
                await Database.set_user_referrer(user_id, referrer_id)
                # Начисляем бонус рефереру
                bonus_granted = await Database.grant_bonus_to_referrer(user_id, REFERRAL_BONUS_DAYS)
                if bonus_granted:
                    try:
                        await context.bot.send_message(
                            chat_id=referrer_id,
                            text=f"🎉 По вашей реферальной ссылке зарегистрировался новый пользователь!\n"
                                 f"Вам начислено {REFERRAL_BONUS_DAYS} дней подписки в подарок!"
                        )
                    except Exception:
                        pass
        except Exception:
            pass

    user = await Database.get_user(user_id)
    if user and user[5] and user[5] not in ('user', None):
        await main_menu(update, context)
        return ConversationHandler.END

    text = (
        "👋 *Добро пожаловать в сервис мониторинга недвижимости*\n\n"
        "🏠 *Находите лучшие предложения быстрее всех!*\n\n"
        "🔹 *Мгновенные уведомления* — новые объявления из ЦИАН и Авито сразу после публикации\n"
        "🔹 *Умные фильтры* — поиск по округам, метро, комнатам и типу сделки\n"
        "🔹 *Только собственники* — отсеивайте агентов\n"
        "🔹 *Реферальная программа* — приглашайте друзей и получайте **14 дней подписки** за каждого!\n\n"
        "💎 *Тарифы:*\n"
        "• 1 месяц — 200 ⭐️ / 1.5 TON / 150 руб\n"
        "• 3 месяца — 500 ⭐️ / 4 TON / 400 руб\n"
        "• 6 месяцев — 950 ⭐️ / 7.5 TON / 750 руб\n"
        "• 12 месяцев — 1800 ⭐️ / 14 TON / 1400 руб\n\n"
        "👇 *Выберите вашу роль:*"
    )

    keyboard = [
        [InlineKeyboardButton("🏢 Агент / Риелтор", callback_data='role_agent')],
        [InlineKeyboardButton("👤 Частное лицо / Покупатель", callback_data='role_owner')]
    ]
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    return ROLE_SELECTION


async def role_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    role = q.data.split('_')[1]

    await Database.set_user_role(user_id, role)

    if role == 'agent':
        text = (
            "🏢 *Вы выбрали роль Агента / Риелтора*\n\n"
            "✨ *Ваши преимущества:*\n"
            "• Мгновенное получение новых объявлений от собственников\n"
            "• Возможность первым связаться с продавцом\n"
            "• Доступ к объявлениям с Авито, которых нет на ЦИАН\n\n"
            "💼 Настройте фильтры и получайте свежие лиды раньше конкурентов."
        )
    else:
        text = (
            "👤 *Вы выбрали роль Частного лица / Покупателя*\n\n"
            "✨ *Ваши преимущества:*\n"
            "• Первыми узнавайте о новых вариантах\n"
            "• Экономия времени на поиске\n"
            "• Только актуальные объявления\n\n"
            "🔍 Настройте фильтры под свои критерии."
        )

    keyboard = [[InlineKeyboardButton("➡️ Перейти в главное меню", callback_data='main_menu')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    return ConversationHandler.END


async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if await Database.is_banned(user_id):
        if update.callback_query:
            await update.callback_query.answer("⛔ Вы забанены.", show_alert=True)
        else:
            await update.effective_message.reply_text("⛔ Вы забанены.")
        return

    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='cp')],
        [InlineKeyboardButton("ℹ️ Мой профиль", callback_data='profile')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='fl')],
        [InlineKeyboardButton("🆘 Поддержка", callback_data='support')],
        [InlineKeyboardButton("❓ Помощь", callback_data='help')]
    ]

    if user_id == ADMIN_ID:
        keyboard.append([InlineKeyboardButton("👑 Админ-панель", callback_data='admin_panel_back')])
    else:
        perms = await Database.is_moderator(user_id)
        if perms:
            keyboard.append([InlineKeyboardButton("🛡 Модератор", callback_data='mod_panel_back')])

    text = "👋 *Главное меню*\n\nВыберите действие:"

    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception:
            await update.callback_query.message.reply_text(
                text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    is_admin = (user_id == ADMIN_ID)
    is_mod = await Database.is_moderator(user_id)

    text = (
        "📚 *Помощь по функциям бота*\n\n"
        "💳 *Подписаться* — выбор тарифа и оплата\n"
        "ℹ️ *Мой профиль* — подписка, фильтры, рефералы, баланс\n"
        "⚙️ *Настроить фильтры* — округа, комнаты, метро, тип сделки, источники\n"
        "🆘 *Поддержка* — написать модератору\n\n"
    )

    if is_mod or is_admin:
        text += (
            "🛡 *Команды модератора:*\n"
            "/mod — панель модератора\n"
            "/tickets — открытые тикеты\n"
            "/reply \\<id\\> \\<текст\\> — ответить пользователю\n"
            "/close\\_ticket \\<id\\> — закрыть тикет\n"
            "/view\\_ticket \\<id\\> — история тикета\n\n"
        )

    if is_admin:
        text += (
            "👑 *Команды администратора:*\n"
            "/admin — панель администратора\n"
            "/act \\<payment\\_id\\> — активировать подписку по TON (ручной режим)\n"
            "/grant \\<id\\> \\<days\\> \\[plan\\] — выдать подписку\n"
            "/stats — статистика\n"
            "/find \\<id\\> — поиск пользователя\n"
            "/ban \\<id\\> / /unban \\<id\\>\n"
            "/set\\_balance \\<id\\> \\<currency\\> \\<amount\\>\n"
            "/add\\_balance \\<id\\> \\<currency\\> \\<amount\\>\n"
            "/export\\_users — CSV экспорт\n"
            "/broadcast \\<текст\\> — рассылка\n"
            "/testparse — тест парсинга\n"
        )

    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]]

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(
            text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await Database.get_user(user_id)
    now = int(time.time())

    if user and user[1] and user[1] > now:
        rem = user[1] - now
        days = rem // 86400
        hours = (rem % 86400) // 3600
        source = user[4] or 'unknown'
        sub_status = f"✅ Активна (осталось {days} дн. {hours} ч.)\nИсточник: {source}"
    else:
        sub_status = "❌ Не активна"

    filters_json = user[0] if user and user[0] else None
    if filters_json:
        try:
            f = json.loads(filters_json)
            districts = ', '.join(f.get('districts', [])) or 'все'
            rooms = ', '.join(f.get('rooms', [])) or 'все'
            metros_list = f.get('metros', [])
            metros = ', '.join(metros_list[:5]) + ('...' if len(metros_list) > 5 else '') if metros_list else 'все'
            sources = ', '.join(f.get('sources', ['cian', 'avito'])) or 'все'
            owner_type = "Только собственники" if f.get('owner_only') else "Все"
            deal_type = DEAL_TYPE_NAMES.get(f.get('deal_type', 'sale'), 'Продажа')
            filters_text = (f"🏘 Округа: {districts}\n🛏 Комнат: {rooms}\n🚇 Метро: {metros}\n"
                            f"📱 Площадки: {sources}\n👤 Тип: {owner_type}\n📋 Сделка: {deal_type}")
        except Exception:
            filters_text = "⚠️ Ошибка в фильтрах"
    else:
        filters_text = "⚙️ Фильтры не настроены"

    user_tg = update.effective_user
    full_name = user_tg.full_name or '—'
    username = f"@{user_tg.username}" if user_tg.username else "не указан"

    # Рефералы (только статистика, без комиссии)
    referrals = await Database.get_referrals(user_id)
    total_refs = len(referrals)
    # Количество рефералов, за которые уже получен бонус
    bonus_refs = sum(1 for r in referrals if r['bonus_granted'])
    referrals_text = f"\n\n📊 *Приглашено друзей:* {total_refs} (получено бонусов: {bonus_refs} x {REFERRAL_BONUS_DAYS} дн.)"

    balance_ton = await Database.get_balance(user_id, 'TON')
    balance_stars = await Database.get_balance(user_id, 'STARS')
    balance_rub = await Database.get_balance(user_id, 'RUB')
    balance_text = (f"\n\n💰 *Ваш баланс:*\n"
                    f"• TON: {balance_ton:.2f}\n"
                    f"• ⭐️ Stars: {balance_stars:.0f}\n"
                    f"• RUB: {balance_rub:.2f}")

    bot_info = await context.bot.get_me()
    ref_link = f"https://t.me/{bot_info.username}?start=ref_{user_id}"

    text = (
        f"👤 *Ваш профиль*\n\n"
        f"🆔 ID: `{user_id}`\n"
        f"📛 Имя: {full_name}\n"
        f"🌐 Username: {username}\n"
        f"🎭 Роль: {user[5] if user else 'user'}\n\n"
        f"📅 *Статус подписки:*\n{sub_status}\n\n"
        f"🔧 *Ваши фильтры:*\n{filters_text}"
        f"{referrals_text}"
        f"{balance_text}\n\n"
        f"🔗 *Реферальная ссылка:*\n`{ref_link}`"
    )

    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]]

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(
            text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


# ========== ПОДПИСКА ==========
async def choose_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    keyboard = [
        [InlineKeyboardButton(
            f"1 месяц — {PRICES_TON['1m']} TON / {PRICES_RUB['1m']} руб / {PRICES_STARS['1m']} ⭐️",
            callback_data='p1m')],
        [InlineKeyboardButton(
            f"3 месяца — {PRICES_TON['3m']} TON / {PRICES_RUB['3m']} руб / {PRICES_STARS['3m']} ⭐️",
            callback_data='p3m')],
        [InlineKeyboardButton(
            f"6 месяцев — {PRICES_TON['6m']} TON / {PRICES_RUB['6m']} руб / {PRICES_STARS['6m']} ⭐️",
            callback_data='p6m')],
        [InlineKeyboardButton(
            f"12 месяцев — {PRICES_TON['12m']} TON / {PRICES_RUB['12m']} руб / {PRICES_STARS['12m']} ⭐️",
            callback_data='p12m')],
        [InlineKeyboardButton("« Назад", callback_data='main_menu')]
    ]
    await q.edit_message_text("📅 Выберите срок подписки:", reply_markup=InlineKeyboardMarkup(keyboard))


async def plan_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    plan = q.data[1:]  # p1m -> 1m
    context.user_data['plan'] = plan
    keyboard = [
        [InlineKeyboardButton("💎 Telegram Stars", callback_data='pay_stars')],
        [InlineKeyboardButton("💰 TON (криптовалюта)", callback_data='pay_ton')],
        [InlineKeyboardButton("💳 Банковская карта (рубли)", callback_data='pay_rub')],
        [InlineKeyboardButton("💼 Оплатить с баланса", callback_data='pay_balance')],
        [InlineKeyboardButton("« Назад", callback_data='cp')]
    ]
    await q.edit_message_text(
        f"Выберите способ оплаты для тарифа *{plan}*:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def pay_stars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    plan = context.user_data.get('plan', '1m')
    amount = PRICES_STARS[plan]
    user_id = q.from_user.id

    payment_id = await Database.add_payment(user_id, amount_stars=amount, plan=plan, source='stars')

    await context.bot.send_invoice(
        chat_id=user_id,
        title=f"Подписка на {plan}",
        description=f"Доступ на {PLAN_DAYS[plan]} дней",
        payload=f"stars_{plan}_{user_id}_{payment_id}",
        provider_token="",
        currency="XTR",
        prices=[LabeledPrice(label="Подписка", amount=amount)],
    )


async def pay_ton(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    plan = context.user_data.get('plan', '1m')
    amount = PRICES_TON[plan]
    payment_id = await Database.add_payment(user_id, amount_ton=amount, plan=plan, source='ton_auto')

    text = (
        f"*Оплата в TON*\n\n"
        f"Сумма: *{amount} TON*\n"
        f"Кошелёк: `{TON_WALLET}`\n\n"
        f"После перевода отправьте команду:\n"
        f"`/pay {payment_id} <TXID>`\n\n"
        f"Платёж будет проверен автоматически. Если всё верно, подписка активируется сразу.\n\n"
        f"ID платежа: `{payment_id}`"
    )
    await q.edit_message_text(text, parse_mode='Markdown')


async def pay_rub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.info(f"PAYMENT_PROVIDER_TOKEN = {PAYMENT_PROVIDER_TOKEN}")

    if not PAYMENT_PROVIDER_TOKEN:
        await update.callback_query.answer(
            "Оплата картой временно недоступна. Выберите другой способ.", show_alert=True)
        return
    q = update.callback_query
    await q.answer()
    plan = context.user_data.get('plan', '1m')
    amount_rub = PRICES_RUB[plan]
    user_id = q.from_user.id

    payment_id = await Database.add_payment(user_id, amount_rub=amount_rub, plan=plan, source='rub_telegram')

    await context.bot.send_invoice(
        chat_id=user_id,
        title=f"Подписка на {plan}",
        description=f"Доступ на {PLAN_DAYS[plan]} дней",
        payload=f"rub_{plan}_{user_id}_{payment_id}",
        provider_token=PAYMENT_PROVIDER_TOKEN,
        currency="RUB",
        prices=[LabeledPrice(label="Подписка", amount=amount_rub * 100)],
    )


async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    await query.answer(ok=True)


async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    payload = update.message.successful_payment.invoice_payload
    parts = payload.split('_')
    currency = update.message.successful_payment.currency

    if currency == "RUB" and len(parts) >= 4 and parts[0] == 'rub':
        plan = parts[1]
        payment_id = int(parts[3])
        days = PLAN_DAYS.get(plan, 30)
        await Database.activate_subscription(user_id, days, plan, source='payment_telegram')
        await Database.confirm_payment(payment_id)
        await update.message.reply_text("✅ Оплата прошла успешно! Подписка активирована.")

    elif currency == "XTR" and len(parts) >= 4 and parts[0] == 'stars':
        plan = parts[1]
        payment_id = int(parts[3])
        days = PLAN_DAYS.get(plan, 30)
        await Database.activate_subscription(user_id, days, plan, source='stars')
        await Database.confirm_payment(payment_id)
        await update.message.reply_text("✅ Оплата звёздами прошла! Подписка активирована.")
    else:
        await update.message.reply_text("✅ Оплата прошла, но возникла ошибка активации. Обратитесь в поддержку.")


async def pay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    try:
        payment_id = int(context.args[0])
        txid = context.args[1]
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /pay <payment_id> <TXID>")
        return

    if not validate_txid(txid):
        await update.message.reply_text("❌ Неверный формат TXID. Ожидается 64 шестнадцатеричных символа.")
        return

    # Получаем информацию о платеже
    async with Database._pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT user_id, plan, amount_ton FROM payments WHERE id=$1 AND status=$2',
            payment_id, 'pending'
        )
        if not row or row['user_id'] != user_id:
            await update.message.reply_text("Платёж не найден или уже обработан.")
            return

        # Проверяем транзакцию через TON API
        expected_amount = float(row['amount_ton'])
        if await verify_ton_transaction(txid, expected_amount):
            # Транзакция подтверждена
            days = PLAN_DAYS.get(row['plan'], 30)
            await Database.activate_subscription(user_id, days, row['plan'], source='ton_auto')
            await conn.execute(
                "UPDATE payments SET status='confirmed', txid=$1, confirmed_at=EXTRACT(EPOCH FROM NOW())::BIGINT WHERE id=$2",
                txid, payment_id
            )
            await update.message.reply_text("✅ Платёж подтверждён! Подписка активирована.")
            # Уведомление админу (опционально)
            await context.bot.send_message(
                chat_id=ADMIN_ID,
                text=f"💰 Пользователь {user_id} автоматически активировал подписку (платёж #{payment_id})"
            )
        else:
            # Транзакция не найдена или неверная
            await conn.execute('UPDATE payments SET txid=$1 WHERE id=$2', txid, payment_id)
            await update.message.reply_text(
                "❌ Не удалось подтвердить транзакцию. Проверьте TXID и повторите попытку позже.\n"
                "Если вы уверены, что перевод выполнен, обратитесь к администратору."
            )


async def pay_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    plan = context.user_data.get('plan', '1m')
    days = PLAN_DAYS[plan]

    balances = {
        'TON': await Database.get_balance(user_id, 'TON'),
        'STARS': await Database.get_balance(user_id, 'STARS'),
        'RUB': await Database.get_balance(user_id, 'RUB')
    }
    required = {
        'TON': PRICES_TON[plan],
        'STARS': PRICES_STARS[plan],
        'RUB': PRICES_RUB[plan]
    }

    available = [(curr, required[curr]) for curr in ['TON', 'STARS', 'RUB'] if balances[curr] >= required[curr]]

    if not available:
        bal_text = "\n".join([f"• {k}: {v:.2f}" for k, v in balances.items()])
        req_text = "\n".join([f"• {k}: {v}" for k, v in required.items()])
        await q.edit_message_text(
            f"❌ Недостаточно средств на балансе.\n\nВаш баланс:\n{bal_text}\n\nНужно:\n{req_text}",
            reply_markup=get_back_keyboard('cp', '« К выбору оплаты')
        )
        return

    if len(available) == 1:
        currency, amount = available[0]
        await Database.deduct_from_balance(user_id, currency, amount)
        await Database.activate_subscription(user_id, days, plan, source=f'balance_{currency}')
        await q.edit_message_text(f"✅ Подписка активирована! Списано {amount} {currency}.")
    else:
        keyboard = []
        for currency, amount in available:
            keyboard.append(
                [InlineKeyboardButton(f"{currency} ({amount})", callback_data=f'balpay_{currency}_{plan}')])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data='cp')])
        await q.edit_message_text("Выберите валюту для оплаты:", reply_markup=InlineKeyboardMarkup(keyboard))


async def balance_pay_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split('_')
    currency = parts[1]
    plan = parts[2]
    days = PLAN_DAYS[plan]
    user_id = q.from_user.id
    amount = {'TON': PRICES_TON[plan], 'STARS': PRICES_STARS[plan], 'RUB': PRICES_RUB[plan]}[currency]

    balance = await Database.get_balance(user_id, currency)
    if balance < amount:
        await q.edit_message_text("❌ Недостаточно средств. Пополните баланс.",
                                  reply_markup=get_back_keyboard())
        return

    await Database.deduct_from_balance(user_id, currency, amount)
    await Database.activate_subscription(user_id, days, plan, source=f'balance_{currency}')
    await q.edit_message_text(f"✅ Подписка активирована! Списано {amount} {currency}.")


# ========== ФИЛЬТРЫ ==========
async def start_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id

    # Загружаем текущие фильтры из БД
    user = await Database.get_user(user_id)
    if user and user[0]:
        try:
            saved = json.loads(user[0])
            context.user_data.setdefault('districts', saved.get('districts', []))
            context.user_data.setdefault('rooms', saved.get('rooms', []))
            context.user_data.setdefault('metros', saved.get('metros', []))
            context.user_data.setdefault('owner_only', saved.get('owner_only', False))
            context.user_data.setdefault('deal_type', saved.get('deal_type', 'sale'))
            context.user_data.setdefault('sources', saved.get('sources', ['cian', 'avito']))
        except Exception:
            pass
    else:
        context.user_data.setdefault('districts', [])
        context.user_data.setdefault('rooms', [])
        context.user_data.setdefault('metros', [])
        context.user_data.setdefault('owner_only', False)
        context.user_data.setdefault('deal_type', 'sale')
        context.user_data.setdefault('sources', ['cian', 'avito'])

    d_count = len(context.user_data.get('districts', []))
    r_count = len(context.user_data.get('rooms', []))
    m_count = len(context.user_data.get('metros', []))

    keyboard = [
        [InlineKeyboardButton(f"🏘 Округа ({d_count})", callback_data='f_districts')],
        [InlineKeyboardButton(f"🛏 Комнаты ({r_count})", callback_data='f_rooms')],
        [InlineKeyboardButton(f"🚇 Метро ({m_count})", callback_data='f_metros')],
        [InlineKeyboardButton("📱 Площадки", callback_data='f_sources')],
        [InlineKeyboardButton("👤 Тип объявлений", callback_data='f_owner')],
        [InlineKeyboardButton("📋 Тип сделки", callback_data='f_deal_type')],
        [InlineKeyboardButton("✅ Сохранить фильтры", callback_data='f_done')],
        [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
    ]

    await q.edit_message_text(
        "⚙️ *Настройка фильтров*\n\nВыберите что настроить:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def filter_districts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    selected = context.user_data.get('districts', [])

    keyboard = []
    for d in DISTRICTS:
        mark = "✅" if d in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {d}", callback_data=f'd_{d}')])
    keyboard.append([InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')])

    await q.edit_message_text("🏘 Выберите округа:", reply_markup=InlineKeyboardMarkup(keyboard))


async def toggle_district(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    district = q.data[2:]
    selected = context.user_data.get('districts', [])

    if district in selected:
        selected.remove(district)
    else:
        selected.append(district)
    context.user_data['districts'] = selected

    keyboard = []
    for d in DISTRICTS:
        mark = "✅" if d in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {d}", callback_data=f'd_{d}')])
    keyboard.append([InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')])
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))


async def filter_rooms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    selected = context.user_data.get('rooms', [])

    keyboard = []
    for r in ROOM_OPTIONS:
        mark = "✅" if r in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {r}", callback_data=f'r_{r}')])
    keyboard.append([InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')])
    await q.edit_message_text("🛏 Выберите количество комнат:", reply_markup=InlineKeyboardMarkup(keyboard))


async def toggle_room(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    room = q.data[2:]
    selected = context.user_data.get('rooms', [])

    if room in selected:
        selected.remove(room)
    else:
        selected.append(room)
    context.user_data['rooms'] = selected

    keyboard = []
    for r in ROOM_OPTIONS:
        mark = "✅" if r in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {r}", callback_data=f'r_{r}')])
    keyboard.append([InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')])
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))


async def filter_metros(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    selected = context.user_data.get('metros', [])

    keyboard = []
    for code, line in METRO_LINES.items():
        # Считаем выбранные станции на этой линии
        count = sum(1 for s in line['stations'] if s in selected)
        suffix = f" ({count})" if count > 0 else ""
        keyboard.append([InlineKeyboardButton(f"{line['name']}{suffix}", callback_data=f'l_{code}')])
    keyboard.append([InlineKeyboardButton("🔍 Поиск по названию", callback_data='metro_search')])
    if selected:
        keyboard.append([InlineKeyboardButton(f"🗑 Сбросить всё ({len(selected)})", callback_data='metro_clear')])
    keyboard.append([InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')])

    await q.edit_message_text(
        f"🚇 Выберите ветку метро:\n_Выбрано станций: {len(selected)}_",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def metro_clear(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer("Метро сброшено")
    context.user_data['metros'] = []
    await filter_metros(update, context)


async def metro_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    line_code = q.data[2:]
    context.user_data['cur_line'] = line_code
    line = METRO_LINES[line_code]
    selected = context.user_data.get('metros', [])

    keyboard = []
    for idx, station in enumerate(line['stations']):
        mark = "✅" if station in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {station}", callback_data=f"m_{line_code}_{idx}")])
    keyboard.append([InlineKeyboardButton("« Назад к веткам", callback_data='f_metros')])

    await q.edit_message_text(
        f"🚇 *{line['name']}*\nВыберите станции:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def toggle_metro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    parts = q.data.split('_')
    line_code = parts[1]
    idx = int(parts[2])
    line = METRO_LINES[line_code]
    station = line['stations'][idx]

    selected = context.user_data.get('metros', [])
    if station in selected:
        selected.remove(station)
    else:
        selected.append(station)
    context.user_data['metros'] = selected

    keyboard = []
    for i, s in enumerate(line['stations']):
        mark = "✅" if s in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {s}", callback_data=f"m_{line_code}_{i}")])
    keyboard.append([InlineKeyboardButton("« Назад к веткам", callback_data='f_metros')])

    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))


async def metro_search_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data['awaiting_metro_search'] = True
    await q.edit_message_text(
        "🔍 Введите название станции (или часть названия):\n\n_Например: Чист, Арбат, Проспект_",
        parse_mode='Markdown'
    )
    return METRO_SEARCH_STATE


async def handle_metro_search_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.lower().strip()
    found = [s for s in ALL_METRO_STATIONS if text in s.lower()]

    if not found:
        await update.message.reply_text(
            "Ничего не найдено. Попробуйте другое название.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("« К веткам", callback_data='f_metros')]])
        )
        return METRO_SEARCH_STATE

    keyboard = []
    for station in found[:10]:
        global_idx = STATION_TO_INDEX[station]
        mark = "✅" if station in context.user_data.get('metros', []) else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {station}", callback_data=f"ms_{global_idx}")])
    keyboard.append([InlineKeyboardButton("« К веткам метро", callback_data='f_metros')])

    await update.message.reply_text(
        f"Найдено {len(found)} станций (показаны первые 10):",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )
    context.user_data['awaiting_metro_search'] = False
    return ConversationHandler.END


async def toggle_metro_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    global_idx = int(q.data.split('_')[1])
    station = INDEX_TO_STATION[global_idx]

    selected = context.user_data.get('metros', [])
    if station in selected:
        selected.remove(station)
        action = "удалена из"
    else:
        selected.append(station)
        action = "добавлена в"
    context.user_data['metros'] = selected

    await q.edit_message_text(
        f"✅ Станция *{station}* {action} фильтр.\nВсего выбрано: {len(selected)}",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Продолжить поиск", callback_data='metro_search')],
            [InlineKeyboardButton("« К веткам метро", callback_data='f_metros')]
        ])
    )


async def filter_sources(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    selected = context.user_data.get('sources', ['cian', 'avito'])

    keyboard = [
        [InlineKeyboardButton(f"{'✅' if 'cian' in selected else '⬜'} ЦИАН", callback_data='src_cian')],
        [InlineKeyboardButton(f"{'✅' if 'avito' in selected else '⬜'} Авито", callback_data='src_avito')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_text("📱 Выберите площадки для мониторинга:", reply_markup=InlineKeyboardMarkup(keyboard))


async def toggle_source(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    source = q.data.split('_')[1]
    selected = context.user_data.get('sources', ['cian', 'avito'])

    if source in selected:
        if len(selected) > 1:  # нельзя убрать все источники
            selected.remove(source)
        else:
            await q.answer("Нужна хотя бы одна площадка!", show_alert=True)
            return
    else:
        selected.append(source)
    context.user_data['sources'] = selected

    keyboard = [
        [InlineKeyboardButton(f"{'✅' if 'cian' in selected else '⬜'} ЦИАН", callback_data='src_cian')],
        [InlineKeyboardButton(f"{'✅' if 'avito' in selected else '⬜'} Авито", callback_data='src_avito')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))


async def filter_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    current = context.user_data.get('owner_only', False)

    keyboard = [
        [InlineKeyboardButton(f"{'✅' if not current else '⬜'} Все объявления", callback_data='owner_all')],
        [InlineKeyboardButton(f"{'✅' if current else '⬜'} Только собственники", callback_data='owner_only')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_text("👤 Выберите тип объявлений:", reply_markup=InlineKeyboardMarkup(keyboard))


async def toggle_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data['owner_only'] = (q.data == 'owner_only')
    current = context.user_data['owner_only']

    keyboard = [
        [InlineKeyboardButton(f"{'✅' if not current else '⬜'} Все объявления", callback_data='owner_all')],
        [InlineKeyboardButton(f"{'✅' if current else '⬜'} Только собственники", callback_data='owner_only')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))


async def filter_deal_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    current = context.user_data.get('deal_type', 'sale')

    keyboard = [
        [InlineKeyboardButton(f"{'✅' if current == 'sale' else '⬜'} Продажа", callback_data='deal_sale')],
        [InlineKeyboardButton(f"{'✅' if current == 'rent' else '⬜'} Аренда", callback_data='deal_rent')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_text("📋 Выберите тип сделки:", reply_markup=InlineKeyboardMarkup(keyboard))


async def toggle_deal_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data['deal_type'] = 'sale' if q.data == 'deal_sale' else 'rent'
    current = context.user_data['deal_type']

    keyboard = [
        [InlineKeyboardButton(f"{'✅' if current == 'sale' else '⬜'} Продажа", callback_data='deal_sale')],
        [InlineKeyboardButton(f"{'✅' if current == 'rent' else '⬜'} Аренда", callback_data='deal_rent')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))


async def filter_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_filter(update, context)


async def filters_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id

    filters_dict = {
        'city': 'Москва',
        'districts': context.user_data.get('districts', []),
        'rooms': context.user_data.get('rooms', []),
        'metros': context.user_data.get('metros', []),
        'owner_only': context.user_data.get('owner_only', False),
        'deal_type': context.user_data.get('deal_type', 'sale'),
        'sources': context.user_data.get('sources', ['cian', 'avito'])
    }

    await Database.set_user_filters(user_id, filters_dict)

    deal_name = DEAL_TYPE_NAMES.get(filters_dict['deal_type'], 'Продажа')
    source_names = {'cian': 'ЦИАН', 'avito': 'Авито'}
    sources_str = ', '.join([source_names.get(s, s) for s in filters_dict['sources']])
    metros_count = len(filters_dict['metros'])

    text = (
        "✅ *Фильтры сохранены!*\n\n"
        f"🏙 Город: Москва\n"
        f"🏘 Округа: {', '.join(filters_dict['districts']) if filters_dict['districts'] else 'все'}\n"
        f"🛏 Комнат: {', '.join(filters_dict['rooms']) if filters_dict['rooms'] else 'все'}\n"
        f"🚇 Метро: {metros_count} станций выбрано\n"
        f"📱 Площадки: {sources_str}\n"
        f"👤 Тип: {'Только собственники' if filters_dict['owner_only'] else 'Все'}\n"
        f"📋 Сделка: {deal_name}\n\n"
        f"Теперь вы будете получать уведомления по этим фильтрам!"
    )

    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


# ========== ПОДДЕРЖКА ==========
async def support_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        q = update.callback_query
        await q.answer()
        context.user_data['awaiting_support'] = True
        await q.edit_message_text(
            "🆘 *Поддержка*\n\nНапишите ваш вопрос или проблему. "
            "Модератор ответит вам в ближайшее время.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]])
        )
    return SUPPORT_STATE


async def handle_support_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message_text = update.message.text

    ticket_id = await Database.get_user_open_ticket(user_id)
    if ticket_id:
        await Database.add_ticket_message(ticket_id, user_id, message_text, is_from_mod=False)
        forward_text = (
            f"💬 *Сообщение в тикете #{ticket_id}*\n"
            f"От: {update.effective_user.full_name} (@{update.effective_user.username})\n"
            f"ID: `{user_id}`\n\n"
            f"*Сообщение:*\n{message_text}"
        )
        await notify_moderators(context.bot, forward_text)
        await update.message.reply_text("✅ Ваше сообщение отправлено модератору.")
    else:
        ticket_id = await Database.create_ticket(user_id, message_text)
        await Database.add_ticket_message(ticket_id, user_id, message_text, is_from_mod=False)
        forward_text = (
            f"🆘 *Новое обращение #{ticket_id}*\n"
            f"От: {update.effective_user.full_name} (@{update.effective_user.username})\n"
            f"ID: `{user_id}`\n\n"
            f"*Сообщение:*\n{message_text}\n\n"
            f"Ответ: `/reply {user_id} <текст>`"
        )
        await notify_moderators(context.bot, forward_text)
        await update.message.reply_text(
            f"✅ Тикет #{ticket_id} создан. Ожидайте ответа модератора.\n\n"
            "Вы можете продолжать писать в этот чат — все сообщения попадут в тикет."
        )

    context.user_data.pop('awaiting_support', None)
    return ConversationHandler.END


# ========== ТИКЕТЫ ==========
async def tickets_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID and not await Database.has_permission(user_id, 'view_tickets'):
        await update.message.reply_text("⛔ Нет прав.")
        return

    tickets = await Database.get_open_tickets()
    if not tickets:
        await update.message.reply_text("Нет открытых тикетов. ✅")
        return

    text = "🆘 *Открытые тикеты:*\n\n"
    for t in tickets:
        time_str = datetime.fromtimestamp(t['created_at']).strftime('%d.%m %H:%M')
        preview = t['message'][:60] + '...' if len(t['message']) > 60 else t['message']
        text += f"*#{t['id']}* | `{t['user_id']}` | {time_str}\n{preview}\n\n"

    await update.message.reply_text(text, parse_mode='Markdown')


async def close_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID and not await Database.has_permission(user_id, 'view_tickets'):
        await update.message.reply_text("⛔ Нет прав.")
        return
    try:
        ticket_id = int(context.args[0])
        await Database.close_ticket(ticket_id)
        await update.message.reply_text(f"✅ Тикет #{ticket_id} закрыт.")
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /close_ticket <id>")


async def close_ticket_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик нажатия кнопки закрытия тикета"""
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    if user_id != ADMIN_ID and not await Database.has_permission(user_id, 'view_tickets'):
        await q.edit_message_text("⛔ Нет прав.")
        return
    try:
        ticket_id = int(q.data.split('_')[-1])
        await Database.close_ticket(ticket_id)
        await q.edit_message_text(f"✅ Тикет #{ticket_id} закрыт.")
    except Exception as e:
        await q.edit_message_text(f"❌ Ошибка: {e}")


async def admin_reply_to_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID and not await Database.has_permission(user_id, 'view_tickets'):
        await update.message.reply_text("⛔ Нет прав.")
        return

    try:
        parts = update.message.text.split(maxsplit=2)
        if len(parts) < 3:
            await update.message.reply_text("Использование: /reply <user_id> <текст>")
            return
        target_user_id = int(parts[1])
        reply_text = parts[2]

        ticket_id = await Database.get_user_open_ticket(target_user_id)
        if not ticket_id:
            ticket_id = await Database.create_ticket(target_user_id, "Сообщение от модератора")

        await Database.add_ticket_message(ticket_id, user_id, reply_text, is_from_mod=True)

        await context.bot.send_message(
            chat_id=target_user_id,
            text=f"📬 *Ответ модератора:*\n{reply_text}",
            parse_mode='Markdown'
        )
        await update.message.reply_text(f"✅ Ответ отправлен пользователю {target_user_id}")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")


async def view_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID and not await Database.has_permission(user_id, 'view_tickets'):
        await update.message.reply_text("⛔ Нет прав.")
        return
    try:
        ticket_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /view_ticket <id>")
        return

    messages = await Database.get_ticket_messages(ticket_id)
    if not messages:
        await update.message.reply_text("Тикет не найден.")
        return

    text = f"📋 *Тикет #{ticket_id}*\n\n"
    for msg in messages:
        sender = "🛡 Модератор" if msg['is_from_mod'] else f"👤 {msg['user_id']}"
        time_str = datetime.fromtimestamp(msg['created_at']).strftime('%d.%m %H:%M')
        preview = msg['message'][:200]
        text += f"[{time_str}] {sender}:\n{preview}\n\n"

    # Добавляем кнопку закрытия
    keyboard = [[InlineKeyboardButton("❌ Закрыть тикет", callback_data=f"close_ticket_{ticket_id}")]]
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


# ========== МОДЕРАТОРСКАЯ ПАНЕЛЬ ==========
async def mod_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    perms = await Database.is_moderator(user_id)
    if not perms:
        if update.callback_query:
            await update.callback_query.answer("⛔ Доступ запрещён.", show_alert=True)
        else:
            await update.message.reply_text("⛔ Доступ запрещён.")
        return

    keyboard = []
    if 'view_tickets' in perms:
        keyboard.append([InlineKeyboardButton("🆘 Открытые тикеты", callback_data='mod_tickets')])
        keyboard.append([InlineKeyboardButton("📋 Закрытые тикеты", callback_data='mod_closed_tickets')])
    if 'view_stats' in perms:
        keyboard.append([InlineKeyboardButton("📊 Статистика", callback_data='mod_stats')])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')])

    text = "🛡 *Панель модератора*"

    if update.callback_query:
        await update.callback_query.edit_message_text(
            text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(
            text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def mod_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    if not await Database.has_permission(user_id, 'view_tickets'):
        return

    tickets = await Database.get_open_tickets()
    if not tickets:
        text = "Нет открытых тикетов. ✅"
    else:
        text = "🆘 *Открытые тикеты:*\n\n"
        for t in tickets:
            time_str = datetime.fromtimestamp(t['created_at']).strftime('%d.%m %H:%M')
            preview = t['message'][:50]
            text += f"*#{t['id']}* | `{t['user_id']}` | {time_str}\n{preview}\n\n"

    keyboard = [[InlineKeyboardButton("« Назад", callback_data='mod_panel_back')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def mod_closed_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    if not await Database.has_permission(user_id, 'view_tickets'):
        return

    tickets = await Database.get_closed_tickets(limit=20)
    if not tickets:
        text = "Нет закрытых тикетов."
    else:
        text = "📋 *Последние закрытые тикеты:*\n\n"
        for t in tickets:
            time_str = datetime.fromtimestamp(t['created_at']).strftime('%d.%m %H:%M')
            text += f"#{t['id']} | `{t['user_id']}` | {time_str}\n"

    keyboard = [[InlineKeyboardButton("« Назад", callback_data='mod_panel_back')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def mod_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    if not await Database.has_permission(user_id, 'view_stats'):
        return

    total, active, pending, *_, open_tickets, ads_count = await Database.get_stats()

    text = (
        f"📊 *Статистика*\n\n"
        f"👥 Всего пользователей: {total}\n"
        f"✅ Активных подписок: {active}\n"
        f"🆘 Открытых тикетов: {open_tickets}\n"
        f"📰 Объявлений в базе: {ads_count}"
    )

    keyboard = [[InlineKeyboardButton("« Назад", callback_data='mod_panel_back')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def mod_panel_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await mod_panel(update, context)


# ========== АДМИН ПАНЕЛЬ ==========
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        if update.callback_query:
            await update.callback_query.answer("⛔ Доступ запрещён.", show_alert=True)
        else:
            await update.message.reply_text("⛔ Доступ запрещён.")
        return

    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data='admin_stats')],
        [InlineKeyboardButton("👥 Список пользователей", callback_data='admin_users_0')],
        [InlineKeyboardButton("🆘 Открытые тикеты", callback_data='admin_tickets')],
        [InlineKeyboardButton("📋 Закрытые тикеты", callback_data='admin_closed_tickets')],
        [InlineKeyboardButton("📢 Рассылка клиентам", callback_data='admin_broadcast')],
        [InlineKeyboardButton("📢 Рассылка модераторам", callback_data='admin_broadcast_mods')],
        [InlineKeyboardButton("🔍 Поиск пользователя", callback_data='admin_find')],
        [InlineKeyboardButton("👥 Активные подписчики", callback_data='admin_active_subs')],
        [InlineKeyboardButton("➕ Добавить модератора", callback_data='admin_add_mod')],
        [InlineKeyboardButton("➖ Удалить модератора", callback_data='admin_remove_mod')],
        [InlineKeyboardButton("📋 Список модераторов", callback_data='admin_list_mods')],
        [InlineKeyboardButton("⚙️ Режим отладки", callback_data='admin_debug')],
        [InlineKeyboardButton("💰 Балансы", callback_data='admin_balances')],
        [InlineKeyboardButton("🚫 Заблокированные", callback_data='admin_banned')],
        [InlineKeyboardButton("📤 Экспорт CSV", callback_data='admin_export')],
        [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
    ]

    text = "🔧 *Админ-панель*"

    if update.callback_query:
        try:
            await update.callback_query.edit_message_text(
                text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
        except Exception:
            await update.callback_query.message.reply_text(
                text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(
            text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_panel_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    context.user_data.pop('awaiting_mod_user_id', None)
    await admin_panel(update, context)


async def admin_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    total, active, pending, total_ton, total_rub, total_stars, monthly_ton, open_tickets, ads_count = await Database.get_stats()

    text = (
        f"📊 *Статистика бота*\n\n"
        f"👥 Всего пользователей: {total}\n"
        f"✅ Активных подписок: {active}\n"
        f"⏳ Ожидают подтверждения: {pending}\n"
        f"💰 Ежемесячный доход (TON): *{monthly_ton:.2f} TON*\n"
        f"💵 Общий доход TON: *{total_ton:.2f}*\n"
        f"💳 Общий доход RUB: *{total_rub} руб*\n"
        f"⭐️ Общий доход Stars: *{total_stars}*\n"
        f"🆘 Открытых тикетов: {open_tickets}\n"
        f"📰 Объявлений в базе: {ads_count}\n"
        f"⏱ Интервал парсинга: {PARSING_INTERVAL // 60} мин"
    )

    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_users_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    parts = q.data.split('_')
    offset = int(parts[2]) if len(parts) > 2 else 0
    rows = await Database.get_all_users(limit=20, offset=offset)

    if not rows:
        await q.edit_message_text("Нет пользователей.",
                                  reply_markup=get_back_keyboard('admin_panel_back', '« Назад'))
        return

    text = f"*Пользователи (страница {offset // 20 + 1}):*\n\n"
    now = int(time.time())
    for row in rows:
        user_id, until, plan, source = row['user_id'], row['subscribed_until'], row['plan'], row['subscription_source']
        if until and until > now:
            remaining = (until - now) // 86400
            status = f"✅ {remaining}д"
        else:
            status = "❌"
        text += f"• `{user_id}` {status} {plan or ''}\n"

    keyboard = []
    nav = []
    if offset >= 20:
        nav.append(InlineKeyboardButton("⬅️ Назад", callback_data=f'admin_users_{offset - 20}'))
    if len(rows) == 20:
        nav.append(InlineKeyboardButton("➡️ Вперёд", callback_data=f'admin_users_{offset + 20}'))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')])

    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    tickets = await Database.get_open_tickets()
    if not tickets:
        text = "Нет открытых тикетов. ✅"
    else:
        text = "🆘 *Открытые тикеты:*\n\n"
        for t in tickets:
            time_str = datetime.fromtimestamp(t['created_at']).strftime('%d.%m %H:%M')
            preview = t['message'][:50]
            text += f"*#{t['id']}* | `{t['user_id']}` | {time_str}\n{preview}\n\n"

    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_closed_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    tickets = await Database.get_closed_tickets(limit=20)
    if not tickets:
        text = "Нет закрытых тикетов."
    else:
        text = "📋 *Закрытые тикеты:*\n\n"
        for t in tickets:
            time_str = datetime.fromtimestamp(t['created_at']).strftime('%d.%m %H:%M')
            text += f"#{t['id']} | `{t['user_id']}` | {time_str}\n"

    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "Используйте команду:\n`/broadcast <текст>` — рассылка всем пользователям",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]]))


async def admin_broadcast_mods_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "Используйте команду:\n`/broadcast_mods <текст>` — рассылка модераторам",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]]))


async def admin_find_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "Используйте команду:\n`/find <user_id>` — найти пользователя",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]]))


async def admin_active_subs_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    rows = await Database.get_active_subscribers_detailed()
    if not rows:
        text = "Нет активных подписчиков."
    else:
        text = f"*Активные подписчики ({len(rows)}):*\n\n"
        now = int(time.time())
        for row in rows[:30]:
            remaining = (row['subscribed_until'] - now) // 86400
            text += f"• `{row['user_id']}` | {row['plan'] or '—'} | {remaining}д | {row['subscription_source'] or '—'}\n"
        if len(rows) > 30:
            text += f"\n_...и ещё {len(rows) - 30}_"

    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_add_mod_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    context.user_data['awaiting_mod_user_id'] = True
    await q.edit_message_text(
        "Введите ID пользователя, которого хотите сделать модератором:",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("« Отмена", callback_data='admin_panel_back')]]))
    return ADD_MOD_STATE


async def admin_handle_add_mod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('awaiting_mod_user_id') or update.effective_user.id != ADMIN_ID:
        return

    try:
        mod_user_id = int(update.message.text.strip())
    except ValueError:
        await update.message.reply_text("❌ Неверный ID. Введите число.")
        return ConversationHandler.END

    context.user_data.pop('awaiting_mod_user_id', None)

    perms = ['view_tickets', 'view_stats']
    await Database.add_moderator(mod_user_id, perms, ADMIN_ID)

    try:
        await context.bot.send_message(
            chat_id=mod_user_id,
            text=(
                "🎉 *Поздравляем! Вы стали модератором.*\n\n"
                "Доступные команды:\n"
                "• /mod — панель модератора\n"
                "• /tickets — список тикетов\n"
                "• /reply \\<id\\> \\<текст\\> — ответить пользователю\n"
                "• /close\\_ticket \\<id\\> — закрыть тикет\n"
                "• /view\\_ticket \\<id\\> — история тикета"
            ),
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить модератора {mod_user_id}: {e}")

    await update.message.reply_text(f"✅ Пользователь {mod_user_id} добавлен как модератор.")
    return ConversationHandler.END


async def admin_remove_mod_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    mods = await Database.get_moderators()
    if not mods:
        await q.edit_message_text("Нет модераторов.",
                                  reply_markup=get_back_keyboard('admin_panel_back', '« Назад'))
        return

    keyboard = []
    for m in mods:
        keyboard.append([InlineKeyboardButton(f"❌ Удалить {m['user_id']}", callback_data=f'rmmod_{m["user_id"]}')])
    keyboard.append([InlineKeyboardButton("« Назад", callback_data='admin_panel_back')])
    await q.edit_message_text("Выберите модератора для удаления:", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_remove_mod_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    mod_id = int(q.data.split('_')[1])
    await Database.remove_moderator(mod_id)
    await q.edit_message_text(
        f"✅ Модератор {mod_id} удалён.",
        reply_markup=get_back_keyboard('admin_panel_back', '« Назад'))


async def admin_list_mods_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    mods = await Database.get_moderators()
    if not mods:
        text = "Нет модераторов."
    else:
        text = "*Список модераторов:*\n\n"
        for m in mods:
            perms = ', '.join(m['permissions']) if m['permissions'] else '—'
            text += f"• `{m['user_id']}` | {perms}\n"

    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_debug_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    debug_mode = context.bot_data.get('debug_mode', False)
    keyboard = [
        [InlineKeyboardButton(f"{'✅' if debug_mode else '⬜'} Включить", callback_data='dbg_on')],
        [InlineKeyboardButton(f"{'✅' if not debug_mode else '⬜'} Выключить", callback_data='dbg_off')],
        [InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]
    ]
    status = "включён" if debug_mode else "выключен"
    await q.edit_message_text(
        f"⚙️ *Режим отладки*\n\nСтатус: {status}\n\nВ этом режиме бот проверяет обновления в GitHub "
        f"и уведомляет вас о новых коммитах.",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


async def admin_debug_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    context.bot_data['debug_mode'] = (q.data == 'dbg_on')
    status = "включён" if context.bot_data['debug_mode'] else "выключен"
    await q.edit_message_text(f"✅ Режим отладки {status}.",
                              reply_markup=get_back_keyboard('admin_panel_back', '« Назад'))


async def admin_balances_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    balances = await Database.get_all_balances()
    if not balances:
        text = "Нет записей о балансах."
    else:
        text = "*Балансы пользователей:*\n\n"
        for b in balances[:30]:
            text += f"• `{b['user_id']}` | {b['currency']}: {float(b['amount']):.2f}\n"
        if len(balances) > 30:
            text += f"\n_...и ещё {len(balances) - 30}_"

    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_banned_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()

    banned = await Database.get_banned_users()
    if not banned:
        text = "Нет заблокированных пользователей. ✅"
    else:
        text = f"*Заблокированные ({len(banned)}):*\n\n"
        for row in banned:
            text += f"• `{row['user_id']}`\n"

    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_export_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "Используйте команду `/export_users` для выгрузки в CSV.",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]]))


# ========== АДМИН КОМАНДЫ ==========
async def activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        payment_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /act <payment_id>")
        return

    payment = await Database.get_pending_payment(payment_id)
    if not payment:
        await update.message.reply_text("Платёж не найден или уже активирован.")
        return

    days = PLAN_DAYS.get(payment['plan'], 30)
    await Database.activate_subscription(payment['user_id'], days, payment['plan'], source='ton_manual')
    await Database.confirm_payment(payment_id)
    await update.message.reply_text(f"✅ Подписка активирована для {payment['user_id']}")

    try:
        await context.bot.send_message(
            chat_id=payment['user_id'],
            text="✅ Ваша подписка активирована! Спасибо за покупку. 🎉"
        )
    except Exception:
        pass


async def grant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        if len(context.args) < 2:
            raise ValueError
        user_id = int(context.args[0])
        days = int(context.args[1])
        plan = context.args[2] if len(context.args) > 2 else None
    except (ValueError, IndexError):
        await update.message.reply_text("Использование: /grant <user_id> <days> [plan]")
        return

    await Database.create_user(user_id)
    await Database.activate_subscription(user_id, days, plan, source='grant')
    await update.message.reply_text(f"✅ Подписка выдана пользователю {user_id} на {days} дней.")

    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=f"🎁 Вам выдана подписка на {days} дней!"
        )
    except Exception:
        pass


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    total, active, pending, total_ton, total_rub, total_stars, monthly_ton, open_tickets, ads_count = await Database.get_stats()
    text = (
        f"📊 *Статистика:*\n\n"
        f"👥 Пользователей: {total}\n"
        f"✅ Активных: {active}\n"
        f"⏳ Ожидают подтверждения: {pending}\n"
        f"💰 Доход TON: {total_ton:.2f}\n"
        f"💳 Доход RUB: {total_rub} руб\n"
        f"⭐️ Доход Stars: {total_stars}\n"
        f"📊 Ежемесячный TON: {monthly_ton:.2f}\n"
        f"🆘 Открытых тикетов: {open_tickets}\n"
        f"📰 Объявлений: {ads_count}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')


async def users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    offset = int(context.args[0]) if context.args else 0
    rows = await Database.get_all_users(limit=20, offset=offset)
    now = int(time.time())
    text = f"*Пользователи (стр. {offset // 20 + 1}):*\n\n"
    for row in rows:
        uid, until, plan, source = row['user_id'], row['subscribed_until'], row['plan'], row['subscription_source']
        if until and until > now:
            remaining = (until - now) // 86400
            status = f"✅ {remaining}д"
        else:
            status = "❌"
        text += f"• `{uid}` {status} {plan or ''}\n"

    keyboard = []
    nav = []
    if offset >= 20:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f'users_page_{offset - 20}'))
    if len(rows) == 20:
        nav.append(InlineKeyboardButton("➡️", callback_data=f'users_page_{offset + 20}'))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🏠 Меню", callback_data='main_menu')])
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def users_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    offset = int(q.data.split('_')[2])
    rows = await Database.get_all_users(limit=20, offset=offset)
    now = int(time.time())
    text = f"*Пользователи (стр. {offset // 20 + 1}):*\n\n"
    for row in rows:
        uid, until, plan, source = row['user_id'], row['subscribed_until'], row['plan'], row['subscription_source']
        if until and until > now:
            remaining = (until - now) // 86400
            status = f"✅ {remaining}д"
        else:
            status = "❌"
        text += f"• `{uid}` {status} {plan or ''}\n"

    keyboard = []
    nav = []
    if offset >= 20:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f'users_page_{offset - 20}'))
    if len(rows) == 20:
        nav.append(InlineKeyboardButton("➡️", callback_data=f'users_page_{offset + 20}'))
    if nav:
        keyboard.append(nav)
    keyboard.append([InlineKeyboardButton("🏠 Меню", callback_data='main_menu')])
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))


async def find_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /find <user_id>")
        return

    user = await Database.get_user(user_id)
    if not user:
        await update.message.reply_text("Пользователь не найден.")
        return

    filters, until, last_ad_id, plan, source, role, referrer_id = user
    now = int(time.time())
    sub_info = f"до {datetime.fromtimestamp(until).strftime('%d.%m.%Y')}" if until and until > now else "нет"

    text = (
        f"👤 *Пользователь {user_id}:*\n\n"
        f"Роль: {role}\n"
        f"Подписка: {sub_info}\n"
        f"План: {plan or '—'}\n"
        f"Источник: {source or '—'}\n"
        f"Реферер: {referrer_id or '—'}\n"
        f"Последнее объявление: {last_ad_id or '—'}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')


async def profile_by_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /profile <user_id>")
        return

    user = await Database.get_user(user_id)
    if not user:
        await update.message.reply_text("Пользователь не найден.")
        return

    now = int(time.time())
    filters_json, until, last_ad_id, plan, source, role, referrer_id = user

    if until and until > now:
        rem = until - now
        sub_status = f"✅ Активна ({rem // 86400}д. {(rem % 86400) // 3600}ч.) | {source}"
    else:
        sub_status = "❌ Не активна"

    balance_ton = await Database.get_balance(user_id, 'TON')
    balance_stars = await Database.get_balance(user_id, 'STARS')
    balance_rub = await Database.get_balance(user_id, 'RUB')

    text = (
        f"👤 *Профиль {user_id}*\n\n"
        f"Роль: {role}\n"
        f"Подписка: {sub_status}\n"
        f"План: {plan or '—'}\n"
        f"Реферер: {referrer_id or '—'}\n\n"
        f"💰 Балансы:\n"
        f"• TON: {balance_ton:.2f}\n"
        f"• Stars: {balance_stars:.0f}\n"
        f"• RUB: {balance_rub:.2f}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')


async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /broadcast <текст>")
        return
    text = ' '.join(context.args)
    context.user_data['broadcast_text'] = text
    keyboard = [
        [InlineKeyboardButton("✅ Подтвердить", callback_data='bc_confirm')],
        [InlineKeyboardButton("❌ Отмена", callback_data='bc_cancel')]
    ]
    await update.message.reply_text(
        f"Текст рассылки:\n\n{text}\n\nПодтвердить отправку?",
        reply_markup=InlineKeyboardMarkup(keyboard))


async def broadcast_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == 'bc_cancel':
        await q.edit_message_text("Рассылка отменена.")
        context.user_data.pop('broadcast_text', None)
        return

    text = context.user_data.get('broadcast_text', '')
    if not text:
        await q.edit_message_text("Ошибка: текст не найден.")
        return

    await q.edit_message_text("📢 Рассылка началась...")
    users = await Database.get_all_users(limit=100000, offset=0)
    sent = 0
    failed = 0
    for row in users:
        try:
            await context.bot.send_message(chat_id=row['user_id'], text=text, parse_mode='Markdown')
            sent += 1
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.debug(f"Рассылка {row['user_id']}: {e}")
            failed += 1

    await q.message.reply_text(f"✅ Рассылка завершена.\nУспешно: {sent}\nОшибок: {failed}")
    context.user_data.pop('broadcast_text', None)


async def broadcast_mods(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /broadcast_mods <текст>")
        return
    text = ' '.join(context.args)
    mods = await Database.get_moderators()
    sent = 0
    for mod in mods:
        try:
            await context.bot.send_message(chat_id=mod['user_id'], text=text, parse_mode='Markdown')
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            pass
    await update.message.reply_text(f"✅ Рассылка модераторам завершена. Отправлено: {sent}")


async def test_parse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("🔄 Запускаю тестовый парсинг...")
    try:
        ads = await fetch_all_ads()
        if ads:
            ad = ads[0]
            text = (
                f"✅ Найдено {len(ads)} объявлений\n\n"
                f"*Первое:*\n"
                f"Источник: {ad.source}\n"
                f"Название: {ad.title[:100]}\n"
                f"Цена: {ad.price}\n"
                f"Адрес: {ad.address[:100]}\n"
                f"Метро: {ad.metro}\n"
                f"Ссылка: {ad.link[:100]}"
            )
        else:
            text = "⚠️ Объявлений не найдено. Проверьте парсеры."
        await update.message.reply_text(text, parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка парсинга: {e}")


async def daily_by_metro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("Функция поиска по метро за сутки в разработке.")


async def admin_active_subs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rows = await Database.get_active_subscribers_detailed()
    if not rows:
        await update.message.reply_text("Нет активных подписчиков.")
        return
    text = f"*Активные подписчики ({len(rows)}):*\n\n"
    now = int(time.time())
    for row in rows[:30]:
        remaining = (row['subscribed_until'] - now) // 86400
        text += f"• `{row['user_id']}` | {row['plan'] or '—'} | {remaining}д | {row['subscription_source'] or '—'}\n"
    await update.message.reply_text(text, parse_mode='Markdown')


async def add_mod_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        mod_user_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /add_mod <user_id>")
        return

    perms = ['view_tickets', 'view_stats']
    await Database.add_moderator(mod_user_id, perms, ADMIN_ID)

    try:
        await context.bot.send_message(
            chat_id=mod_user_id,
            text="🎉 *Вы стали модератором!*\n\nИспользуйте /mod для доступа к панели.",
            parse_mode='Markdown'
        )
    except Exception:
        pass

    await update.message.reply_text(f"✅ Модератор {mod_user_id} добавлен.")


async def remove_mod_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        mod_user_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /remove_mod <user_id>")
        return
    await Database.remove_moderator(mod_user_id)
    await update.message.reply_text(f"✅ Модератор {mod_user_id} удалён.")


async def mods_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    mods = await Database.get_moderators()
    if not mods:
        await update.message.reply_text("Нет модераторов.")
        return
    text = "*Модераторы:*\n\n"
    for m in mods:
        text += f"• `{m['user_id']}` | {', '.join(m['permissions'])}\n"
    await update.message.reply_text(text, parse_mode='Markdown')


async def debug_on_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    context.bot_data['debug_mode'] = True
    await update.message.reply_text("✅ Режим отладки включён.")


async def debug_off_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    context.bot_data['debug_mode'] = False
    await update.message.reply_text("✅ Режим отладки выключен.")


async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /ban <user_id>")
        return
    await Database.ban_user(user_id)
    await update.message.reply_text(f"🚫 Пользователь {user_id} заблокирован.")


async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /unban <user_id>")
        return
    await Database.unban_user(user_id)
    await update.message.reply_text(f"✅ Пользователь {user_id} разблокирован.")


async def set_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        currency = context.args[1].upper()
        amount = float(context.args[2])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /set_balance <user_id> <currency> <amount>")
        return
    await Database.set_balance(user_id, currency, amount)
    await update.message.reply_text(f"✅ Баланс {user_id} в {currency} установлен на {amount}.")


async def add_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        currency = context.args[1].upper()
        amount = float(context.args[2])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /add_balance <user_id> <currency> <amount>")
        return
    await Database.add_to_balance(user_id, currency, amount)
    await update.message.reply_text(f"✅ Добавлено {amount} {currency} пользователю {user_id}.")


async def export_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    users = await Database.get_all_users(limit=100000, offset=0)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['user_id', 'subscribed_until', 'plan', 'subscription_source'])
    for row in users:
        writer.writerow([row['user_id'], row['subscribed_until'], row['plan'], row['subscription_source']])
    output.seek(0)
    await update.message.reply_document(
        document=output.getvalue().encode('utf-8'),
        filename=f'users_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    )


# ========== ФОНОВЫЕ ЗАДАЧИ ==========
def matches_filters(ad: Ad, filters_dict: dict) -> bool:
    """Проверяет, подходит ли объявление под фильтры пользователя"""
    sources = filters_dict.get('sources', ['cian', 'avito'])
    if ad.source not in sources:
        return False

    deal_type = filters_dict.get('deal_type', 'sale')
    if ad.deal_type != deal_type:
        return False

    districts = filters_dict.get('districts', [])
    if districts:
        # Если округ определён и он не в списке – не подходит
        if ad.district_detected and ad.district_detected not in districts:
            return False
        # Если округ не определён – пропускаем (не исключаем)

    metros = filters_dict.get('metros', [])
    if metros and ad.metro and ad.metro != 'Не указано':
        ad_metro_clean = ad.metro.lower().replace('м.', '').strip()
        found = any(m.lower() in ad_metro_clean or ad_metro_clean in m.lower() for m in metros)
        if not found:
            return False

    rooms = filters_dict.get('rooms', [])
    if rooms:
        room_type = None
        rc = str(ad.rooms).lower().strip()
        if rc == 'студия':
            room_type = 'Студия'
        elif rc == '1':
            room_type = '1-комнатная'
        elif rc == '2':
            room_type = '2-комнатная'
        elif rc == '3':
            room_type = '3-комнатная'
        elif rc.isdigit() and int(rc) >= 4:
            room_type = '4-комнатная+'
        if room_type not in rooms:
            return False

    owner_only = filters_dict.get('owner_only', False)
    if owner_only and not ad.owner:
        return False

    return True


async def send_ad_to_user(bot, user_id: int, ad: Ad, telegram_semaphore):
    """Отправляет объявление пользователю (атомарная проверка дублей)"""
    async with telegram_semaphore:
        # Атомарно пытаемся добавить запись об отправке
        async with Database._pool.acquire() as conn:
            inserted = await conn.fetchval(
                '''
                INSERT INTO sent_ads (user_id, ad_id)
                VALUES ($1, $2)
                ON CONFLICT (user_id, ad_id) DO NOTHING
                RETURNING id
                ''',
                user_id, ad.id
            )
            if not inserted:
                # Уже отправляли этому пользователю
                return

        owner_text = "👤 Собственник" if ad.owner else "🏢 Агент"
        deal_text = "Продажа" if ad.deal_type == 'sale' else "Аренда"
        source_icon = "🏢" if ad.source == 'cian' else "📱"
        source_name = "ЦИАН" if ad.source == 'cian' else "Авито"

        # Экранируем поля для Markdown
        safe_title = escape_markdown(ad.title)
        safe_price = escape_markdown(ad.price)
        safe_address = escape_markdown(ad.address)
        safe_metro = escape_markdown(ad.metro)
        safe_floor = escape_markdown(ad.floor)
        safe_area = escape_markdown(ad.area)
        safe_rooms = escape_markdown(ad.rooms)

        text = (
            f"🏠 *Новое объявление* от {source_icon} {source_name}\n"
            f"💰 *Цена:* {safe_price}\n"
            f"📍 *Адрес:* {safe_address}\n"
            f"🚇 *Метро:* {safe_metro}\n"
            f"🏢 *Этаж:* {safe_floor}\n"
            f"📏 *Площадь:* {safe_area}\n"
            f"🛏 *Комнат:* {safe_rooms}\n"
            f"👤 *Тип:* {owner_text} | {deal_text}\n"
            f"\n[🔗 Открыть объявление]({ad.link})"
        )

        try:
            if ad.photos and len(ad.photos) > 0:
                valid_photos = [p for p in ad.photos[:5] if p.startswith('http')]
                if valid_photos:
                    media = [InputMediaPhoto(
                        media=valid_photos[0],
                        caption=text,
                        parse_mode='Markdown'
                    )]
                    for photo_url in valid_photos[1:]:
                        media.append(InputMediaPhoto(media=photo_url))
                    await bot.send_media_group(chat_id=user_id, media=media)
                else:
                    await bot.send_message(
                        chat_id=user_id,
                        text=text,
                        parse_mode='Markdown',
                        disable_web_page_preview=False
                    )
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode='Markdown',
                    disable_web_page_preview=False
                )

            await asyncio.sleep(0.1)

        except Exception as e:
            logger.error(f"Ошибка отправки пользователю {user_id}: {e}")


async def collector_loop(app: Application):
    """Фоновая задача: сбор объявлений и рассылка подписчикам"""
    telegram_semaphore = app.bot_data.get('telegram_semaphore')

    while True:
        try:
            logger.info("Запуск сбора объявлений...")
            subscribers = await Database.get_active_subscribers()
            if not subscribers:
                logger.info("Нет активных подписчиков")
                await asyncio.sleep(PARSING_INTERVAL)
                continue

            ads = await fetch_all_ads()
            if not ads:
                logger.info("Нет новых объявлений")
                await asyncio.sleep(PARSING_INTERVAL)
                continue

            new_ads = []
            for ad in ads:
                if await Database.save_ad(ad):
                    new_ads.append(ad)

            if not new_ads:
                logger.info("Нет новых объявлений после дедупликации")
                await asyncio.sleep(PARSING_INTERVAL)
                continue

            logger.info(f"Найдено {len(new_ads)} новых объявлений, рассылаем...")

            # Собираем все задачи для всех объявлений и пользователей
            all_tasks = []
            for ad in new_ads:
                for row in subscribers:
                    user_id = row['user_id']
                    filters_json = row['filters']
                    if not filters_json:
                        continue
                    try:
                        f = json.loads(filters_json)
                    except Exception:
                        continue

                    if matches_filters(ad, f):
                        all_tasks.append(send_ad_to_user(app.bot, user_id, ad, telegram_semaphore))

            if all_tasks:
                await asyncio.gather(*all_tasks, return_exceptions=True)

            # Очистка старых объявлений раз в день
            await Database.cleanup_old_ads(days=30)
            logger.info(f"Рассылка завершена. Отправлено задач: {len(all_tasks)}")

        except Exception as e:
            logger.error(f"Ошибка в collector_loop: {e}", exc_info=True)

        await asyncio.sleep(PARSING_INTERVAL)


async def update_checker_loop(app: Application):
    """Проверка обновлений в GitHub (только уведомление, без авто-перезапуска)"""
    while True:
        try:
            debug_mode = app.bot_data.get('debug_mode', False)
            if debug_mode:
                logger.info("Проверка обновлений GitHub...")
                try:
                    proc = await asyncio.create_subprocess_exec(
                        'git', 'rev-parse', 'HEAD',
                        cwd='/opt/bot',
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout, _ = await proc.communicate()
                    current_commit = stdout.decode().strip()

                    proc2 = await asyncio.create_subprocess_exec(
                        'git', 'ls-remote', 'origin', GITHUB_BRANCH,
                        cwd='/opt/bot',
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE
                    )
                    stdout2, _ = await proc2.communicate()
                    remote_output = stdout2.decode().strip()
                    remote_commit = remote_output.split()[0] if remote_output else ''

                    if remote_commit and remote_commit != current_commit:
                        await app.bot.send_message(
                            chat_id=ADMIN_ID,
                            text=(
                                f"🔄 *Доступно обновление!*\n\n"
                                f"Текущий: `{current_commit[:8]}`\n"
                                f"Новый: `{remote_commit[:8]}`\n\n"
                                f"Для обновления выполните `git pull` вручную."
                            ),
                            parse_mode='Markdown'
                        )
                except Exception as e:
                    logger.error(f"Ошибка проверки обновлений: {e}")
        except Exception as e:
            logger.error(f"Ошибка в update_checker_loop: {e}")

        await asyncio.sleep(AUTO_UPDATE_CHECK_INTERVAL)


# ========== ConversationHandler объекты ==========
SUPPORT_CONV = ConversationHandler(
    entry_points=[CallbackQueryHandler(support_start, pattern='^support$')],
    states={
        SUPPORT_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_support_message)]
    },
    fallbacks=[],
    allow_reentry=True
)

METRO_SEARCH_CONV = ConversationHandler(
    entry_points=[CallbackQueryHandler(metro_search_start, pattern='^metro_search$')],
    states={
        METRO_SEARCH_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_metro_search_text)]
    },
    fallbacks=[],
    allow_reentry=True
)

ADD_MOD_CONV = ConversationHandler(
    entry_points=[CallbackQueryHandler(admin_add_mod_callback, pattern='^admin_add_mod$')],
    states={
        ADD_MOD_STATE: [MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_add_mod)]
    },
    fallbacks=[],
    allow_reentry=True
)