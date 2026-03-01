import json
import logging
import asyncio
import time
import re
import sys
import csv
import io
from datetime import datetime
from typing import List, Dict, Optional
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, LabeledPrice
from telegram.ext import ContextTypes, ConversationHandler, Application
from telegram.constants import ParseMode

from config import (
    ADMIN_ID, TON_WALLET, PAYMENT_PROVIDER_TOKEN, PRICES_RUB, PRICES_TON, PRICES_STARS,
    PLAN_DAYS, REFERRAL_COMMISSION, PARSING_INTERVAL, TELEGRAM_RATE_LIMIT,
    DISTRICTS, ROOM_OPTIONS, DEAL_TYPE_NAMES, METRO_LINES, ALL_METRO_STATIONS,
    GITHUB_REPO, GITHUB_BRANCH, GITHUB_TOKEN, AUTO_UPDATE_CHECK_INTERVAL
)
from database import Database
from models import Ad
from parsers import fetch_all_ads, fetch_cian_deal_type, fetch_avito_deal_type
from utils import validate_txid, truncate_text, check_user_exists

logger = logging.getLogger(__name__)

# Состояния для ConversationHandler
ROLE_SELECTION = 0

# Глобальные словари для метро (поиск по индексу)
STATION_TO_INDEX = {station: idx for idx, station in enumerate(ALL_METRO_STATIONS)}
INDEX_TO_STATION = {idx: station for station, idx in STATION_TO_INDEX.items()}

# ========== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ==========
async def notify_moderators(bot, text: str):
    """Уведомляет всех модераторов с правом view_tickets"""
    await bot.send_message(chat_id=ADMIN_ID, text=text, parse_mode='Markdown')
    mods = await Database.get_moderators()
    for mod in mods:
        if await Database.has_permission(mod['user_id'], 'view_tickets'):
            try:
                await bot.send_message(chat_id=mod['user_id'], text=text, parse_mode='Markdown')
            except:
                pass

async def handle_referral_commission(referred_user_id: int, plan: str, currency: str, amount_paid: float, bot):
    """Начисляет комиссию рефереру"""
    user = await Database.get_user(referred_user_id)
    if not user or not user[6]:
        return
    referrer_id = user[6]
    commission = amount_paid * (REFERRAL_COMMISSION / 100)

    async with Database._pool.acquire() as conn:
        await conn.execute('''
            UPDATE referrals SET commission_paid=TRUE, payment_amount=$1, currency=$3
            WHERE referred_id=$2
        ''', commission, referred_user_id, currency)
    
    await Database.add_to_balance(referrer_id, currency, commission)

    try:
        await bot.send_message(
            chat_id=referrer_id,
            text=f"💰 Вам начислена комиссия {commission:.2f} {currency} за подписку реферала (ID {referred_user_id})."
        )
    except Exception as e:
        logger.error(f"Ошибка уведомления реферера {referrer_id}: {e}")

# ========== ГЛАВНОЕ МЕНЮ И СТАРТ ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start с маркетинговым текстом и выбором роли"""
    user_id = update.effective_user.id
    if await Database.is_banned(user_id):
        await update.message.reply_text("⛔ Вы забанены. Обратитесь в поддержку.")
        return ConversationHandler.END

    await Database.create_user(user_id)
    
    # Парсинг реферального параметра
    args = context.args
    if args and args[0].startswith('ref_'):
        try:
            referrer_id = int(args[0].split('_')[1])
            if referrer_id != user_id:
                await Database.set_user_referrer(user_id, referrer_id)
                await context.bot.send_message(
                    chat_id=referrer_id,
                    text=f"🎉 По вашей реферальной ссылке зарегистрировался новый пользователь {user_id}!"
                )
        except:
            pass
    
    # Проверяем, есть ли уже роль
    user = await Database.get_user(user_id)
    if user and user[5] != 'user' and user[5] is not None:
        await main_menu(update, context)
        return ConversationHandler.END

    # Маркетинговый текст с преимуществами
    text = (
        "👋 *Добро пожаловать в сервис мониторинга недвижимости*\n\n"
        "🏠 *Находите лучшие предложения быстрее всех!*\n\n"
        "🔹 *Мгновенные уведомления* – получайте новые объявления из ЦИАН и Авито сразу после публикации.\n"
        "🔹 *Умные фильтры* – настраивайте поиск по округам, станциям метро, количеству комнат и типу сделки.\n"
        "🔹 *Только собственники* – отсеивайте агентов и экономьте на комиссии.\n"
        "🔹 *Реферальная система* – приглашайте друзей и получайте 50% от их подписки на свой баланс.\n"
        "🔹 *Безопасный парсинг* – мы используем технологии, которые не блокируются сайтами.\n\n"
        "💎 *Тарифы:*\n"
        "• 1 месяц – 200 ⭐️ / 1.5 TON / 150 руб\n"
        "• 3 месяца – 500 ⭐️ / 4 TON / 400 руб\n"
        "• 6 месяцев – 950 ⭐️ / 7.5 TON / 750 руб\n"
        "• 12 месяцев – 1800 ⭐️ / 14 TON / 1400 руб\n\n"
        "👇 *Выберите вашу роль, чтобы продолжить:*"
    )
    
    keyboard = [
        [InlineKeyboardButton("🏢 Агент / Риелтор", callback_data='role_agent')],
        [InlineKeyboardButton("👤 Частное лицо / Покупатель", callback_data='role_owner')]
    ]
    
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    return ROLE_SELECTION

async def role_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик выбора роли"""
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
            "• Доступ к эксклюзивным предложениям с Avito, которых нет на ЦИАН\n"
            "• Аналитика рынка и ценообразования\n"
            "• Инструменты для работы с клиентами\n\n"
            "💼 *Как использовать:* настройте фильтры и получайте свежие лиды раньше конкурентов."
        )
    else:
        text = (
            "👤 *Вы выбрали роль Частного лица / Покупателя*\n\n"
            "✨ *Ваши преимущества:*\n"
            "• Первыми узнавайте о новых вариантах\n"
            "• Экономия времени на поиске\n"
            "• Только актуальные объявления\n"
            "• Мониторинг цен на интересующие объекты\n"
            "• Уведомления о снижении цены\n\n"
            "🔍 *Как использовать:* настройте фильтры под свои критерии и будьте в курсе всех новинок."
        )

    keyboard = [[InlineKeyboardButton("➡️ Перейти в главное меню", callback_data='main_menu')]]
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    return ConversationHandler.END

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Главное меню бота"""
    user_id = update.effective_user.id
    if await Database.is_banned(user_id):
        await update.effective_message.reply_text("⛔ Вы забанены.")
        return

    user = await Database.get_user(user_id)
    role = user[5] if user else 'user'

    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='cp')],
        [InlineKeyboardButton("ℹ️ Мой профиль", callback_data='profile')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='fl')],
        [InlineKeyboardButton("🆘 Поддержка", callback_data='support')],
        [InlineKeyboardButton("❓ Помощь", callback_data='help')]
    ]
    
    if user_id == ADMIN_ID:
        keyboard.append([InlineKeyboardButton("👑 Админ-панель", callback_data='admin_panel')])
    else:
        perms = await Database.is_moderator(user_id)
        if perms:
            keyboard.append([InlineKeyboardButton("🛡 Модератор", callback_data='mod_panel')])

    text = "👋 *Главное меню*\n\nВыберите действие:"
    
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Справка по командам"""
    user_id = update.effective_user.id
    is_admin = (user_id == ADMIN_ID)
    is_mod = await Database.is_moderator(user_id)

    text = (
        "📚 *Помощь по функциям бота*\n\n"
        "💳 *Подписаться* – выбор тарифа и оплата.\n"
        "ℹ️ *Мой профиль* – информация о подписке, фильтрах, рефералах, балансе.\n"
        "⚙️ *Настроить фильтры* – выбор округов, комнат, метро, типа объявлений, типа сделки, источников.\n"
        "🆘 *Поддержка* – связаться с модератором.\n\n"
    )
    
    if is_mod or is_admin:
        text += "🛡 *Команды модератора:*\n"
        text += "/mod – панель модератора\n"
        text += "/tickets – список открытых тикетов\n"
        text += "/reply <id> <текст> – ответить пользователю\n"
        text += "/close_ticket <id> – закрыть тикет\n"
        text += "/view_ticket <id> – история переписки по тикету\n\n"
    
    if is_admin:
        text += (
            "👑 *Команды администратора:*\n"
            "/admin – панель администратора\n"
            "/act <payment_id> – активировать подписку по TON\n"
            "/grant <id> <days> [plan] – выдать подписку\n"
            "/stats – статистика\n"
            "/users [offset] – список пользователей\n"
            "/find <id> – поиск пользователя\n"
            "/profile <id> – профиль пользователя\n"
            "/tickets – открытые тикеты\n"
            "/close_ticket <id> – закрыть тикет\n"
            "/reply <id> <текст> – ответить пользователю\n"
            "/broadcast – массовая рассылка клиентам\n"
            "/broadcast_mods – рассылка модераторам\n"
            "/testparse – тест парсинга\n"
            "/daily <станции> – поиск за сутки по метро\n"
            "/active_subs – активные подписчики\n"
            "/add_mod <id> – добавить модератора\n"
            "/remove_mod <id> – удалить модератора\n"
            "/mods – список модераторов\n"
            "/debug_on – включить автообновление\n"
            "/debug_off – выключить автообновление\n"
            "/pay <payment_id> <txid> – для пользователей (ручная оплата TON)\n"
            "/ban <id> – забанить пользователя\n"
            "/unban <id> – разбанить\n"
            "/set_balance <id> <currency> <amount> – установить баланс\n"
            "/add_balance <id> <currency> <amount> – добавить на баланс\n"
            "/export_users – экспорт пользователей в CSV\n"
        )
    
    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]]
    
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Профиль пользователя с отображением баланса"""
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
    
    filters = user[0] if user and user[0] else None
    if filters:
        try:
            f = json.loads(filters)
            districts = ', '.join(f.get('districts', [])) or 'все'
            rooms = ', '.join(f.get('rooms', [])) or 'все'
            metros = ', '.join(f.get('metros', [])) or 'все'
            sources = ', '.join(f.get('sources', ['cian', 'avito'])) or 'все'
            owner_type = "Только собственники" if f.get('owner_only') else "Все"
            deal_type = DEAL_TYPE_NAMES.get(f.get('deal_type', 'sale'), 'Продажа')
            filters_text = (f"🏘 Округа: {districts}\n🛏 Комнат: {rooms}\n🚇 Метро: {metros}\n"
                            f"📱 Площадки: {sources}\n👤 Тип: {owner_type}\n📋 Сделка: {deal_type}")
        except:
            filters_text = "⚠️ Ошибка в фильтрах"
    else:
        filters_text = "⚙️ Фильтры не настроены"
    
    user_tg = update.effective_user
    full_name = user_tg.full_name
    username = f"@{user_tg.username}" if user_tg.username else "не указан"

    # Информация о рефералах
    referrals_text = ""
    referrals = await Database.get_referrals(user_id)
    if referrals:
        ref_list = "\n".join([f"• {r['referred_id']} – {datetime.fromtimestamp(r['created_at']).strftime('%d.%m.%Y')} (комиссия: {r['payment_amount']} {r['currency']})" for r in referrals])
        referrals_text = f"\n\n📊 *Ваши рефералы:*\n{ref_list}"

    # Баланс в разных валютах
    balance_ton = await Database.get_balance(user_id, 'TON')
    balance_stars = await Database.get_balance(user_id, 'STARS')
    balance_rub = await Database.get_balance(user_id, 'RUB')
    balance_text = f"\n\n💰 *Ваш баланс:*\n• TON: {balance_ton:.2f}\n• ⭐️ Stars: {balance_stars:.2f}\n• RUB: {balance_rub:.2f}"

    # Реферальная ссылка
    ref_link = f"https://t.me/{(await context.bot.get_me()).username}?start=ref_{user_id}"
    
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
        f"🔗 *Ваша реферальная ссылка:*\n`{ref_link}`"
    )
    
    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]]
    
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

# ---------- ПОДПИСКА ----------
async def choose_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Выбор тарифа подписки"""
    q = update.callback_query
    await q.answer()
    keyboard = [
        [InlineKeyboardButton(f"1 месяц – {PRICES_TON['1m']} TON / {PRICES_RUB['1m']} руб / {PRICES_STARS['1m']} ⭐️", callback_data='p1m')],
        [InlineKeyboardButton(f"3 месяца – {PRICES_TON['3m']} TON / {PRICES_RUB['3m']} руб / {PRICES_STARS['3m']} ⭐️", callback_data='p3m')],
        [InlineKeyboardButton(f"6 месяцев – {PRICES_TON['6m']} TON / {PRICES_RUB['6m']} руб / {PRICES_STARS['6m']} ⭐️", callback_data='p6m')],
        [InlineKeyboardButton(f"12 месяцев – {PRICES_TON['12m']} TON / {PRICES_RUB['12m']} руб / {PRICES_STARS['12m']} ⭐️", callback_data='p12m')],
        [InlineKeyboardButton("« Назад", callback_data='main_menu')]
    ]
    await q.edit_message_text("📅 Выберите срок подписки:", reply_markup=InlineKeyboardMarkup(keyboard))

async def plan_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """После выбора плана предлагаем выбрать способ оплаты"""
    q = update.callback_query
    await q.answer()
    plan = q.data[1:]  # p1m -> 1m
    context.user_data['plan'] = plan
    keyboard = [
        [InlineKeyboardButton("💎 Telegram Stars", callback_data='pay_stars')],
        [InlineKeyboardButton("💰 TON (криптовалюта)", callback_data='pay_ton')],
        [InlineKeyboardButton("💳 Банковская карта (рубли)", callback_data='pay_rub')],
        [InlineKeyboardButton("💰 Оплатить с баланса", callback_data='pay_balance')],
        [InlineKeyboardButton("« Назад", callback_data='cp')]
    ]
    await q.edit_message_text(f"Выберите способ оплаты для тарифа {plan}:", reply_markup=InlineKeyboardMarkup(keyboard))

async def pay_stars(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Оплата звёздами Telegram"""
    q = update.callback_query
    await q.answer()
    plan = context.user_data.get('plan', '1m')
    amount = PRICES_STARS[plan]
    user_id = q.from_user.id

    title = f"Подписка на {plan}"
    description = f"Доступ на {PLAN_DAYS[plan]} дней"
    payload = f"stars_{plan}_{user_id}_{int(time.time())}"

    await context.bot.send_invoice(
        chat_id=user_id,
        title=title,
        description=description,
        payload=payload,
        provider_token="",  # Для звёзд токен не нужен
        currency="XTR",
        prices=[LabeledPrice(label="Подписка", amount=amount)],
        start_parameter="subscription_stars"
    )

async def pay_ton(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ручная оплата в TON"""
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    plan = context.user_data.get('plan', '1m')
    amount = PRICES_TON[plan]
    payment_id = await Database.add_payment(user_id, amount_ton=amount, amount_rub=0, amount_stars=0, plan=plan, source='ton_manual')

    text = (
        f"**Оплата в TON**\n\n"
        f"Сумма: **{amount} TON**\n"
        f"Кошелёк: `{TON_WALLET}`\n\n"
        f"**После перевода отправьте команду:**\n"
        f"`/pay {payment_id} <TXID>`\n\n"
        f"Модератор проверит и активирует подписку.\n\n"
        f"**ID платежа:** `{payment_id}`"
    )
    await q.edit_message_text(text, parse_mode='Markdown')

async def pay_rub(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Оплата рублями через Telegram Payments"""
    if not PAYMENT_PROVIDER_TOKEN:
        await update.callback_query.answer("Оплата картой временно недоступна. Выберите другой способ.", show_alert=True)
        return
    q = update.callback_query
    await q.answer()
    plan = context.user_data.get('plan', '1m')
    amount_rub = PRICES_RUB[plan]
    user_id = q.from_user.id

    title = f"Подписка на {plan}"
    description = f"Доступ на {PLAN_DAYS[plan]} дней"
    payload = f"rub_{plan}_{user_id}_{int(time.time())}"
    currency = "RUB"
    prices = [LabeledPrice(label="Подписка", amount=amount_rub * 100)]

    await context.bot.send_invoice(
        chat_id=user_id,
        title=title,
        description=description,
        payload=payload,
        provider_token=PAYMENT_PROVIDER_TOKEN,
        currency=currency,
        prices=prices,
        start_parameter="subscription"
    )

async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Предварительная проверка платежа"""
    query = update.pre_checkout_query
    await query.answer(ok=True)

async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка успешной оплаты (рубли или звёзды)"""
    user_id = update.effective_user.id
    payload = update.message.successful_payment.invoice_payload
    parts = payload.split('_')
    currency = update.message.successful_payment.currency

    if currency == "RUB":
        if len(parts) >= 2 and parts[0] == 'rub':
            plan = parts[1]
            days = PLAN_DAYS.get(plan, 30)
            await Database.activate_subscription(user_id, days, plan, source='payment_telegram')
            amount = PRICES_RUB[plan]
            await handle_referral_commission(user_id, plan, 'RUB', amount, context.bot)
            await update.message.reply_text("✅ Оплата прошла успешно! Подписка активирована.")

    elif currency == "XTR":
        if len(parts) >= 2 and parts[0] == 'stars':
            plan = parts[1]
            days = PLAN_DAYS.get(plan, 30)
            await Database.activate_subscription(user_id, days, plan, source='stars')
            amount = PRICES_STARS[plan]
            await handle_referral_commission(user_id, plan, 'STARS', amount, context.bot)
            await update.message.reply_text("✅ Оплата звёздами прошла успешно! Подписка активирована.")
    else:
        await update.message.reply_text("✅ Оплата прошла, но возникла ошибка с активацией. Обратитесь в поддержку.")

async def pay_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для отправки TXID при ручной оплате TON"""
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

    async with Database._pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT user_id FROM payments WHERE id=$1 AND status=$2',
            payment_id, 'pending'
        )
        if not row or row['user_id'] != user_id:
            await update.message.reply_text("Платёж не найден или уже обработан.")
            return

        await conn.execute('UPDATE payments SET txid=$1 WHERE id=$2', txid, payment_id)

    await update.message.reply_text("✅ TXID сохранён. Администратор проверит и активирует подписку.")
    await context.bot.send_message(
        chat_id=ADMIN_ID,
        text=f"Пользователь {user_id} отправил TXID для платежа #{payment_id}: {txid}"
    )

# ---------- ОПЛАТА С БАЛАНСА ----------
async def pay_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Оплата подписки с внутреннего баланса"""
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
        await q.edit_message_text("❌ Недостаточно средств на балансе.")
        return
    
    if len(available) == 1:
        currency, amount = available[0]
        await Database.deduct_from_balance(user_id, currency, amount)
        await Database.activate_subscription(user_id, days, plan, source=f'balance_{currency}')
        await handle_referral_commission(user_id, plan, currency, amount, context.bot)
        await q.edit_message_text(f"✅ Подписка активирована! Списано {amount} {currency}.")
    else:
        keyboard = []
        for currency, amount in available:
            keyboard.append([InlineKeyboardButton(f"{currency} ({amount})", callback_data=f'balance_pay_{currency}_{plan}')])
        keyboard.append([InlineKeyboardButton("« Назад", callback_data='cp')])
        await q.edit_message_text("Выберите валюту для оплаты:", reply_markup=InlineKeyboardMarkup(keyboard))

async def balance_pay_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    data = q.data.split('_')
    currency = data[2]
    plan = data[3]
    days = PLAN_DAYS[plan]
    user_id = q.from_user.id
    amount = {'TON': PRICES_TON[plan], 'STARS': PRICES_STARS[plan], 'RUB': PRICES_RUB[plan]}[currency]
    
    balance = await Database.get_balance(user_id, currency)
    if balance < amount:
        await q.edit_message_text("❌ Недостаточно средств. Пополните баланс.")
        return
    
    await Database.deduct_from_balance(user_id, currency, amount)
    await Database.activate_subscription(user_id, days, plan, source=f'balance_{currency}')
    await handle_referral_commission(user_id, plan, currency, amount, context.bot)
    await q.edit_message_text(f"✅ Подписка активирована! Списано {amount} {currency}.")

# ---------- ФИЛЬТРЫ ----------
async def start_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало настройки фильтров"""
    q = update.callback_query
    await q.answer()
    
    context.user_data['districts'] = []
    context.user_data['rooms'] = []
    context.user_data['metros'] = []
    context.user_data['owner_only'] = False
    context.user_data['deal_type'] = 'sale'
    context.user_data['sources'] = ['cian', 'avito']
    
    keyboard = [
        [InlineKeyboardButton("🏘 Выбрать округа", callback_data='f_districts')],
        [InlineKeyboardButton("🛏 Выбрать комнаты", callback_data='f_rooms')],
        [InlineKeyboardButton("🚇 Выбрать метро", callback_data='f_metros')],
        [InlineKeyboardButton("📱 Выбрать площадки", callback_data='f_sources')],
        [InlineKeyboardButton("👤 Выбрать тип", callback_data='f_owner')],
        [InlineKeyboardButton("📋 Тип сделки", callback_data='f_deal_type')],
        [InlineKeyboardButton("✅ Завершить настройку", callback_data='f_done')],
        [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
    ]
    
    await q.edit_message_text(
        "⚙️ **Настройка фильтров**\nВыберите, что хотите настроить:",
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
    
    await q.edit_message_text("🏘 Выберите округа (можно несколько):", reply_markup=InlineKeyboardMarkup(keyboard))

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
    
    await q.edit_message_text("🏘 Выберите округа:", reply_markup=InlineKeyboardMarkup(keyboard))

async def filter_rooms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    selected = context.user_data.get('rooms', [])
    
    keyboard = []
    for r in ROOM_OPTIONS:
        mark = "✅" if r in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {r}", callback_data=f'r_{r}')])
    keyboard.append([InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')])
    
    await q.edit_message_text("🛏 Выберите количество комнат (можно несколько):", reply_markup=InlineKeyboardMarkup(keyboard))

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
    
    await q.edit_message_text("🛏 Выберите количество комнат:", reply_markup=InlineKeyboardMarkup(keyboard))

async def filter_metros(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    
    keyboard = []
    for code, line in METRO_LINES.items():
        keyboard.append([InlineKeyboardButton(line['name'], callback_data=f'l_{code}')])
    keyboard.append([InlineKeyboardButton("🔍 Поиск по названию", callback_data='metro_search')])
    keyboard.append([InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')])
    
    await q.edit_message_text("🚇 Выберите ветку метро или найдите по названию:", reply_markup=InlineKeyboardMarkup(keyboard))

async def metro_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает станции выбранной линии (с использованием индексов)"""
    q = update.callback_query
    await q.answer()
    line_code = q.data[2:]
    context.user_data['cur_line'] = line_code
    line = METRO_LINES[line_code]
    selected = context.user_data.get('metros', [])
    
    keyboard = []
    for idx, station in enumerate(line['stations']):
        mark = "✅" if station in selected else "⬜"
        callback_data = f"m_{line_code}_{idx}"
        keyboard.append([InlineKeyboardButton(f"{mark} {station}", callback_data=callback_data)])
    keyboard.append([InlineKeyboardButton("« Назад к веткам", callback_data='f_metros')])
    
    await q.edit_message_text(
        f"🚇 **{line['name']}**\nВыберите станции:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def toggle_metro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Переключает станцию по индексу"""
    q = update.callback_query
    await q.answer()
    data = q.data.split('_')
    line_code = data[1]
    idx = int(data[2])
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
    
    await q.edit_message_text(
        f"🚇 **{line['name']}**\nВыберите станции:",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def metro_search_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text("Введите название станции (или часть названия):")
    context.user_data['awaiting_metro_search'] = True

async def handle_metro_search_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка текстового поиска метро"""
    if not context.user_data.get('awaiting_metro_search'):
        return
    
    text = update.message.text.lower()
    found = []
    for station in ALL_METRO_STATIONS:
        if text in station.lower():
            found.append(station)
    
    if not found:
        await update.message.reply_text("Ничего не найдено. Попробуйте снова.")
        return
    
    keyboard = []
    for station in found[:10]:
        # Используем глобальный индекс для callback
        global_idx = STATION_TO_INDEX[station]
        keyboard.append([InlineKeyboardButton(station, callback_data=f"ms_{global_idx}")])
    keyboard.append([InlineKeyboardButton("« Отмена", callback_data='f_metros')])
    
    await update.message.reply_text("Найдено станций. Выберите:", reply_markup=InlineKeyboardMarkup(keyboard))
    context.user_data['awaiting_metro_search'] = False

async def toggle_metro_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора станции из результатов поиска"""
    q = update.callback_query
    await q.answer()
    global_idx = int(q.data.split('_')[1])
    station = INDEX_TO_STATION[global_idx]
    
    selected = context.user_data.get('metros', [])
    if station in selected:
        selected.remove(station)
    else:
        selected.append(station)
    context.user_data['metros'] = selected
    
    await q.edit_message_text(
        f"✅ Станция {station} добавлена в фильтр.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Продолжить поиск", callback_data='metro_search')],
            [InlineKeyboardButton("« К веткам метро", callback_data='f_metros')]
        ])
    )

async def filter_sources(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    selected = context.user_data.get('sources', ['cian', 'avito'])
    
    text = "📱 Выберите площадки для мониторинга:\n"
    keyboard = [
        [InlineKeyboardButton(f"{'✅' if 'cian' in selected else '⬜'} ЦИАН", callback_data='src_cian')],
        [InlineKeyboardButton(f"{'✅' if 'avito' in selected else '⬜'} Авито", callback_data='src_avito')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def toggle_source(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    source = q.data.split('_')[1]
    selected = context.user_data.get('sources', ['cian', 'avito'])
    
    if source in selected:
        selected.remove(source)
    else:
        selected.append(source)
    context.user_data['sources'] = selected
    
    text = "📱 Выберите площадки для мониторинга:\n"
    keyboard = [
        [InlineKeyboardButton(f"{'✅' if 'cian' in selected else '⬜'} ЦИАН", callback_data='src_cian')],
        [InlineKeyboardButton(f"{'✅' if 'avito' in selected else '⬜'} Авито", callback_data='src_avito')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def filter_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    current = context.user_data.get('owner_only', False)
    
    text = "👤 Выберите тип объявлений:\n"
    keyboard = [
        [InlineKeyboardButton("✅ Все (агенты и собственники)" if not current else "⬜ Все", callback_data='owner_all')],
        [InlineKeyboardButton("✅ Только собственники" if current else "⬜ Только собственники", callback_data='owner_only')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def toggle_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == 'owner_all':
        context.user_data['owner_only'] = False
    elif q.data == 'owner_only':
        context.user_data['owner_only'] = True
    
    current = context.user_data.get('owner_only', False)
    text = "👤 Выберите тип объявлений:\n"
    keyboard = [
        [InlineKeyboardButton("✅ Все" if not current else "⬜ Все", callback_data='owner_all')],
        [InlineKeyboardButton("✅ Только собственники" if current else "⬜ Только собственники", callback_data='owner_only')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def filter_deal_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    current = context.user_data.get('deal_type', 'sale')
    
    text = "📋 Выберите тип сделки:\n"
    keyboard = [
        [InlineKeyboardButton("✅ Продажа" if current == 'sale' else "⬜ Продажа", callback_data='deal_sale')],
        [InlineKeyboardButton("✅ Аренда" if current == 'rent' else "⬜ Аренда", callback_data='deal_rent')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def toggle_deal_type(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == 'deal_sale':
        context.user_data['deal_type'] = 'sale'
    elif q.data == 'deal_rent':
        context.user_data['deal_type'] = 'rent'
    
    current = context.user_data.get('deal_type', 'sale')
    text = "📋 Выберите тип сделки:\n"
    keyboard = [
        [InlineKeyboardButton("✅ Продажа" if current == 'sale' else "⬜ Продажа", callback_data='deal_sale')],
        [InlineKeyboardButton("✅ Аренда" if current == 'rent' else "⬜ Аренда", callback_data='deal_rent')],
        [InlineKeyboardButton("« Назад к фильтрам", callback_data='f_back')]
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def filter_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await start_filter(update, context)

async def filters_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    
    districts = context.user_data.get('districts', [])
    rooms = context.user_data.get('rooms', [])
    metros = context.user_data.get('metros', [])
    owner_only = context.user_data.get('owner_only', False)
    deal_type = context.user_data.get('deal_type', 'sale')
    sources = context.user_data.get('sources', ['cian', 'avito'])
    
    filters = {
        'city': 'Москва',
        'districts': districts,
        'rooms': rooms,
        'metros': metros,
        'owner_only': owner_only,
        'deal_type': deal_type,
        'sources': sources
    }
    
    await Database.set_user_filters(user_id, filters)
    
    deal_name = DEAL_TYPE_NAMES.get(deal_type, 'Продажа')
    source_names = {'cian': 'ЦИАН', 'avito': 'Авито'}
    sources_str = ', '.join([source_names.get(s, s) for s in sources])
    
    text = "✅ **Фильтры сохранены!**\n\n🏙 Город: Москва\n"
    text += f"🏘 Округа: {', '.join(districts) if districts else 'все'}\n"
    text += f"🛏 Комнат: {', '.join(rooms) if rooms else 'все'}\n"
    text += f"🚇 Метро: {', '.join(metros) if metros else 'все'}\n"
    text += f"📱 Площадки: {sources_str}\n"
    text += f"👤 Тип: {'Только собственники' if owner_only else 'Все'}\n"
    text += f"📋 Сделка: {deal_name}"
    
    await q.edit_message_text(text, parse_mode='Markdown')
    await main_menu(update, context)

# ---------- ПОДДЕРЖКА ----------
async def support_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "🆘 Напишите ваш вопрос или проблему. Модератор ответит вам в ближайшее время."
    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]]
    
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    
    context.user_data['awaiting_support'] = True

async def handle_support_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка входящих сообщений в поддержку"""
    if context.user_data.get('awaiting_metro_search') or context.user_data.get('awaiting_mod_user_id'):
        return
    
    user_id = update.effective_user.id
    message_text = update.message.text
    
    # Проверяем, есть ли уже открытый тикет у пользователя
    ticket_id = await Database.get_user_open_ticket(user_id)
    if ticket_id:
        # Добавляем сообщение в существующий тикет
        await Database.add_ticket_message(ticket_id, user_id, message_text, is_from_mod=False)
        forward_text = (
            f"💬 *Новое сообщение по тикету #{ticket_id}*\n"
            f"От: {update.effective_user.full_name} (@{update.effective_user.username})\n"
            f"ID: `{user_id}`\n\n"
            f"*Сообщение:*\n{message_text}"
        )
        await notify_moderators(context.bot, forward_text)
        await update.message.reply_text("✅ Ваше сообщение отправлено модератору.")
    else:
        # Создаём новый тикет
        ticket_id = await Database.create_ticket(user_id, message_text)
        await Database.add_ticket_message(ticket_id, user_id, message_text, is_from_mod=False)
        forward_text = (
            f"🆘 *Новое обращение в поддержку*\n"
            f"От: {update.effective_user.full_name} (@{update.effective_user.username})\n"
            f"ID: `{user_id}`\n"
            f"Тикет #{ticket_id}\n\n"
            f"*Сообщение:*\n{message_text}"
        )
        await notify_moderators(context.bot, forward_text)
        await update.message.reply_text("✅ Ваше сообщение отправлено модератору. Ожидайте ответа.")

async def tickets_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Список открытых тикетов"""
    user_id = update.effective_user.id
    if user_id != ADMIN_ID and not await Database.has_permission(user_id, 'view_tickets'):
        await update.message.reply_text("⛔ У вас нет прав на эту команду.")
        return
    
    tickets = await Database.get_open_tickets()
    if not tickets:
        await update.message.reply_text("Нет открытых тикетов.")
        return
    
    text = "🆘 *Открытые тикеты:*\n\n"
    for t in tickets:
        text += f"#{t['id']} от `{t['user_id']}`: {t['message'][:50]}...\n"
    
    await update.message.reply_text(text, parse_mode='Markdown')

async def close_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if user_id != ADMIN_ID and not await Database.has_permission(user_id, 'view_tickets'):
        await update.message.reply_text("⛔ У вас нет прав на эту команду.")
        return
    
    try:
        ticket_id = int(context.args[0])
        await Database.close_ticket(ticket_id)
        await update.message.reply_text(f"Тикет #{ticket_id} закрыт.")
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /close_ticket id")

async def admin_reply_to_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Ответить пользователю по тикету (сохраняется история)"""
    user_id = update.effective_user.id
    if user_id != ADMIN_ID and not await Database.has_permission(user_id, 'view_tickets'):
        await update.message.reply_text("⛔ У вас нет прав на эту команду.")
        return
    
    try:
        parts = update.message.text.split(maxsplit=2)
        if len(parts) < 3:
            await update.message.reply_text("Использование: /reply user_id текст")
            return
        
        target_user_id = int(parts[1])
        reply_text = parts[2]
        
        # Находим открытый тикет этого пользователя
        ticket_id = await Database.get_user_open_ticket(target_user_id)
        if not ticket_id:
            # Если нет открытого, создаём новый
            ticket_id = await Database.create_ticket(target_user_id, "Ответ модератора (новый тикет)")
        
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
    """Просмотр истории тикета"""
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
    text = f"📋 *История тикета #{ticket_id}*\n\n"
    for msg in messages:
        sender = "Модератор" if msg['is_from_mod'] else f"Пользователь {msg['user_id']}"
        time_str = datetime.fromtimestamp(msg['created_at']).strftime('%d.%m %H:%M')
        text += f"[{time_str}] {sender}: {msg['message'][:100]}...\n"
    await update.message.reply_text(text, parse_mode='Markdown')

# ---------- МОДЕРАТОРСКАЯ ПАНЕЛЬ ----------
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
    
    if update.callback_query:
        await update.callback_query.edit_message_text(
            "🛡 *Панель модератора*",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await update.message.reply_text(
            "🛡 *Панель модератора*",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def mod_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await Database.has_permission(user_id, 'view_tickets'):
        return
    
    q = update.callback_query
    await q.answer()
    
    tickets = await Database.get_open_tickets()
    if not tickets:
        text = "Нет открытых тикетов."
    else:
        text = "🆘 *Открытые тикеты:*\n\n"
        for t in tickets:
            text += f"#{t['id']} от `{t['user_id']}`: {t['message'][:50]}...\n"
    
    await q.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад", callback_data='mod_panel_back')]]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def mod_closed_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await Database.has_permission(user_id, 'view_tickets'):
        return
    q = update.callback_query
    await q.answer()
    tickets = await Database.get_closed_tickets(limit=20, offset=0)
    if not tickets:
        text = "Нет закрытых тикетов."
    else:
        text = "📋 *Последние закрытые тикеты:*\n\n"
        for t in tickets:
            time_str = datetime.fromtimestamp(t['created_at']).strftime('%d.%m %H:%M')
            text += f"#{t['id']} от `{t['user_id']}` ({time_str})\n"
    await q.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад", callback_data='mod_panel_back')]]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def mod_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await Database.has_permission(user_id, 'view_stats'):
        return
    
    q = update.callback_query
    await q.answer()
    
    total, active, pending, total_income_ton, total_income_rub, total_income_stars, monthly_ton, open_tickets, ads_count = await Database.get_stats()
    
    text = (
        f"📊 **Статистика для модератора**\n\n"
        f"👥 Всего пользователей: {total}\n"
        f"✅ Активных подписок: {active}\n"
        f"🆘 Открытых тикетов: {open_tickets}\n"
        f"📰 Объявлений в базе: {ads_count}\n\n"
        f"💰 Доход скрыт (нет прав)."
    )
    
    await q.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад", callback_data='mod_panel_back')]]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def mod_panel_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await Database.is_moderator(user_id):
        return
    await mod_panel(update, context)

# ========== АДМИНСКИЕ ОБРАБОТЧИКИ ==========
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Панель администратора"""
    if update.effective_user.id != ADMIN_ID:
        if update.callback_query:
            await update.callback_query.answer("⛔ Доступ запрещён.", show_alert=True)
        else:
            await update.message.reply_text("⛔ Доступ запрещён.")
        return
    
    if update.callback_query:
        message = update.callback_query
        edit = True
    else:
        message = update.message
        edit = False
    
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
        [InlineKeyboardButton("💰 Балансы пользователей", callback_data='admin_balances')],
        [InlineKeyboardButton("🚫 Заблокированные", callback_data='admin_banned')],
        [InlineKeyboardButton("📤 Экспорт пользователей", callback_data='admin_export')],
        [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
    ]
    
    if edit:
        await message.edit_message_text(
            "🔧 *Админ-панель*",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )
    else:
        await message.reply_text(
            "🔧 *Админ-панель*",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

async def admin_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    total, active, pending, total_income_ton, total_income_rub, total_income_stars, monthly_ton, open_tickets, ads_count = await Database.get_stats()
    
    text = (
        f"📊 **Статистика бота**\n\n"
        f"👥 Всего пользователей: {total}\n"
        f"✅ Активных подписок: {active}\n"
        f"💰 Ежемесячный доход (TON): **{monthly_ton:.2f} TON**\n"
        f"💵 Общий доход (TON): **{total_income_ton:.2f} TON**\n"
        f"💳 Общий доход (RUB): **{total_income_rub} руб**\n"
        f"⭐️ Общий доход (STARS): **{total_income_stars} ⭐️**\n"
        f"⏳ Ожидают подтверждения: {pending}\n"
        f"🆘 Открытых тикетов: {open_tickets}\n"
        f"📰 Объявлений в базе: {ads_count}\n"
        f"⏱ Интервал парсинга: {PARSING_INTERVAL//60} мин\n"
        f"🛡 Защита от флуда: {TELEGRAM_RATE_LIMIT} одновременных"
    )
    
    await q.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_users_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    offset = int(q.data.split('_')[2]) if len(q.data.split('_')) > 2 else 0
    rows = await Database.get_all_users(limit=20, offset=offset)
    
    if not rows:
        await q.edit_message_text("Нет пользователей.")
        return
    
    text = f"**Список пользователей (страница {offset//20 + 1}):**\n\n"
    now = int(time.time())
    for user_id, until, plan, source in rows:
        if until and until > now:
            remaining = (until - now) // 86400
            status = f"✅ (осталось {remaining} дн.)"
        else:
            status = "❌ не активна"
        text += f"• `{user_id}` {status} {plan or ''} ({source or '—'})\n"
    
    keyboard = []
    if offset >= 20:
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f'admin_users_{offset-20}')])
    if len(rows) == 20:
        keyboard.append([InlineKeyboardButton("Вперёд ➡️", callback_data=f'admin_users_{offset+20}')])
    keyboard.append([InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')])
    
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    tickets = await Database.get_open_tickets()
    if not tickets:
        text = "Нет открытых тикетов."
    else:
        text = "🆘 *Открытые тикеты:*\n\n"
        for t in tickets:
            text += f"#{t['id']} от `{t['user_id']}`: {t['message'][:50]}...\n"
    
    await q.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_closed_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    tickets = await Database.get_closed_tickets(limit=20, offset=0)
    if not tickets:
        text = "Нет закрытых тикетов."
    else:
        text = "📋 *Закрытые тикеты:*\n\n"
        for t in tickets:
            time_str = datetime.fromtimestamp(t['created_at']).strftime('%d.%m %H:%M')
            text += f"#{t['id']} от `{t['user_id']}` ({time_str})\n"
    await q.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    await q.edit_message_text(
        "Используйте команду /broadcast <текст> для рассылки клиентам.\n\nНапример: /broadcast Всем привет!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]])
    )

async def admin_broadcast_mods_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    await q.edit_message_text(
        "Используйте команду /broadcast_mods <текст> для рассылки модераторам.\n\nНапример: /broadcast_mods Всем модераторам!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]])
    )

async def admin_find_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    await q.edit_message_text(
        "Используйте команду /find <user_id> для поиска пользователя.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]])
    )

async def admin_active_subs_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    rows = await Database.get_active_subscribers_detailed()
    if not rows:
        text = "Нет активных подписчиков."
    else:
        text = "**Активные подписчики:**\n\n"
        for row in rows:
            user_id = row['user_id']
            until = row['subscribed_until']
            plan = row['plan'] or '—'
            source = row['subscription_source'] or 'unknown'
            remaining = (until - int(time.time())) // 86400
            text += f"• `{user_id}` | {plan} | осталось {remaining} дн. | источник: {source}\n"
    
    await q.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_add_mod_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    context.user_data['awaiting_mod_user_id'] = True
    await q.edit_message_text(
        "Введите ID пользователя, которого хотите сделать модератором:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Отмена", callback_data='admin_panel_back')]])
    )

async def admin_handle_add_mod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('awaiting_mod_user_id') or update.effective_user.id != ADMIN_ID:
        return
    
    try:
        user_id = int(update.message.text.strip())
        # Проверяем существование пользователя в Telegram
        if not await check_user_exists(context.bot, user_id):
            await update.message.reply_text("❌ Пользователь с таким ID не найден в Telegram.")
            context.user_data.pop('awaiting_mod_user_id', None)
            return
        
        perms = ['view_tickets', 'view_stats']
        await Database.add_moderator(user_id, perms, ADMIN_ID)
        
        # Уведомляем нового модератора
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=(
                    "🎉 *Поздравляем! Вы стали модератором.*\n\n"
                    "Теперь вам доступны следующие команды:\n"
                    "• `/mod` – панель модератора\n"
                    "• `/tickets` – список открытых тикетов\n"
                    "• `/reply <id> <текст>` – ответить пользователю\n"
                    "• `/close_ticket <id>` – закрыть тикет\n"
                    "• `/view_ticket <id>` – история переписки по тикету\n\n"
                    "Пожалуйста, используйте свои права ответственно."
                ),
                parse_mode='Markdown'
            )
        except Exception as e:
            logger.error(f"Не удалось уведомить нового модератора {user_id}: {e}")

        await update.message.reply_text(f"✅ Пользователь {user_id} добавлен как модератор с правами: {perms}")
        context.user_data.pop('awaiting_mod_user_id', None)
        await admin_panel(update, context)
    except ValueError:
        await update.message.reply_text("❌ Неверный ID. Введите число.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def admin_remove_mod_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    mods = await Database.get_moderators()
    if not mods:
        await q.edit_message_text(
            "Нет модераторов.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]])
        )
        return
    
    keyboard = []
    for m in mods:
        keyboard.append([InlineKeyboardButton(f"Удалить {m['user_id']}", callback_data=f'remove_mod_{m["user_id"]}')])
    keyboard.append([InlineKeyboardButton("« Назад", callback_data='admin_panel_back')])
    
    await q.edit_message_text("Выберите модератора для удаления:", reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_remove_mod_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    user_id = int(q.data.split('_')[2])
    await Database.remove_moderator(user_id)
    await q.edit_message_text(
        f"✅ Модератор {user_id} удалён.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]])
    )

async def admin_list_mods_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    mods = await Database.get_moderators()
    if not mods:
        text = "Нет модераторов."
    else:
        text = "**Список модераторов:**\n\n"
        for m in mods:
            text += f"• `{m['user_id']}` | права: {', '.join(m['permissions'])}\n"
    
    await q.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_debug_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    debug_mode = context.bot_data.get('debug_mode', False)
    keyboard = [
        [InlineKeyboardButton(f"{'✅' if debug_mode else '⬜'} Включить", callback_data='debug_on')],
        [InlineKeyboardButton(f"{'✅' if not debug_mode else '⬜'} Выключить", callback_data='debug_off')],
        [InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]
    ]
    
    status = "включён" if debug_mode else "выключен"
    await q.edit_message_text(
        f"⚙️ *Режим отладки*\n\nТекущий статус: {status}\n\nВ этом режиме бот каждые 2 минуты проверяет обновления в GitHub и автоматически применяет их (перезапускается).",
        parse_mode='Markdown',
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

async def admin_debug_toggle(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    if q.data == 'debug_on':
        context.bot_data['debug_mode'] = True
        await q.edit_message_text("✅ Режим отладки включён. Проверка обновлений каждые 2 минуты.")
    else:
        context.bot_data['debug_mode'] = False
        await q.edit_message_text("✅ Режим отладки выключен.")
    
    await asyncio.sleep(2)
    await admin_panel(update, context)

async def admin_balances_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    
    balances = await Database.get_all_balances()
    if not balances:
        text = "Нет записей о балансах."
    else:
        text = "**Балансы пользователей:**\n\n"
        for b in balances:
            text += f"• `{b['user_id']}` | {b['currency']}: {b['amount']:.2f} (обновлено {b['updated_at'].strftime('%d.%m %H:%M')})\n"
    
    await q.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await q.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_banned_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    # Заглушка – можно реализовать позже
    await q.edit_message_text(
        "Функция просмотра забаненных пользователей в разработке.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]])
    )

async def admin_export_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    await q.edit_message_text(
        "Используйте команду /export_users для выгрузки пользователей в CSV.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]])
    )

async def admin_panel_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат в админ-панель"""
    if update.effective_user.id != ADMIN_ID:
        return
    context.user_data.pop('awaiting_mod_user_id', None)
    await admin_panel(update, context)

# ---------- АДМИНСКИЕ КОМАНДЫ (ЧЕРЕЗ /) ----------
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
    await update.message.reply_text(f"✅ Подписка активирована для пользователя {payment['user_id']}")
    await context.bot.send_message(
        chat_id=payment['user_id'],
        text="✅ Ваша подписка активирована администратором! Спасибо за покупку."
    )

async def grant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        if len(context.args) < 2:
            await update.message.reply_text("Использование: /grant <user_id> <days> [plan]")
            return
        user_id = int(context.args[0])
        days = int(context.args[1])
        plan = context.args[2] if len(context.args) > 2 else None
    except ValueError:
        await update.message.reply_text("Неверные аргументы.")
        return
    await Database.activate_subscription(user_id, days, plan, source='grant')
    await update.message.reply_text(f"✅ Подписка выдана пользователю {user_id} на {days} дней.")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    total, active, pending, total_income_ton, total_income_rub, total_income_stars, monthly_ton, open_tickets, ads_count = await Database.get_stats()
    text = (
        f"📊 *Статистика:*\n\n"
        f"👥 Всего пользователей: {total}\n"
        f"✅ Активных подписок: {active}\n"
        f"⏳ Ожидают подтверждения: {pending}\n"
        f"💰 Доход TON: {total_income_ton:.2f}\n"
        f"💰 Доход RUB: {total_income_rub} руб\n"
        f"💰 Доход STARS: {total_income_stars} ⭐️\n"
        f"📊 Ежемесячный TON: {monthly_ton:.2f}\n"
        f"🆘 Открытых тикетов: {open_tickets}\n"
        f"📰 Объявлений в базе: {ads_count}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')

async def users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    offset = int(context.args[0]) if context.args else 0
    rows = await Database.get_all_users(limit=20, offset=offset)
    text = f"**Пользователи (страница {offset//20+1}):**\n\n"
    now = int(time.time())
    for user_id, until, plan, source in rows:
        if until and until > now:
            remaining = (until - now) // 86400
            status = f"✅ (осталось {remaining} дн.)"
        else:
            status = "❌ не активна"
        text += f"• `{user_id}` {status} {plan or ''} ({source or '—'})\n"
    keyboard = []
    if offset >= 20:
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f'users_page_{offset-20}')])
    if len(rows) == 20:
        keyboard.append([InlineKeyboardButton("➡️ Вперёд", callback_data=f'users_page_{offset+20}')])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')])
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def users_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    offset = int(q.data.split('_')[2])
    rows = await Database.get_all_users(limit=20, offset=offset)
    text = f"**Пользователи (страница {offset//20+1}):**\n\n"
    now = int(time.time())
    for user_id, until, plan, source in rows:
        if until and until > now:
            remaining = (until - now) // 86400
            status = f"✅ (осталось {remaining} дн.)"
        else:
            status = "❌ не активна"
        text += f"• `{user_id}` {status} {plan or ''} ({source or '—'})\n"
    keyboard = []
    if offset >= 20:
        keyboard.append([InlineKeyboardButton("⬅️ Назад", callback_data=f'users_page_{offset-20}')])
    if len(rows) == 20:
        keyboard.append([InlineKeyboardButton("➡️ Вперёд", callback_data=f'users_page_{offset+20}')])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')])
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
    text = (
        f"**Пользователь {user_id}:**\n\n"
        f"Роль: {role}\n"
        f"Подписка до: {datetime.fromtimestamp(until).strftime('%Y-%m-%d %H:%M') if until else 'нет'}\n"
        f"План: {plan}\n"
        f"Источник: {source}\n"
        f"Реферер: {referrer_id}\n"
        f"Фильтры: {filters}\n"
        f"Последнее объявление: {last_ad_id}"
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
    if user and user[1] and user[1] > now:
        rem = user[1] - now
        days = rem // 86400
        hours = (rem % 86400) // 3600
        source = user[4] or 'unknown'
        sub_status = f"✅ Активна (осталось {days} дн. {hours} ч.)\nИсточник: {source}"
    else:
        sub_status = "❌ Не активна"
    
    filters = user[0] if user and user[0] else None
    if filters:
        try:
            f = json.loads(filters)
            districts = ', '.join(f.get('districts', [])) or 'все'
            rooms = ', '.join(f.get('rooms', [])) or 'все'
            metros = ', '.join(f.get('metros', [])) or 'все'
            sources = ', '.join(f.get('sources', ['cian', 'avito'])) or 'все'
            owner_type = "Только собственники" if f.get('owner_only') else "Все"
            deal_type = DEAL_TYPE_NAMES.get(f.get('deal_type', 'sale'), 'Продажа')
            filters_text = (f"🏘 Округа: {districts}\n🛏 Комнат: {rooms}\n🚇 Метро: {metros}\n"
                            f"📱 Площадки: {sources}\n👤 Тип: {owner_type}\n📋 Сделка: {deal_type}")
        except:
            filters_text = "⚠️ Ошибка в фильтрах"
    else:
        filters_text = "⚙️ Фильтры не настроены"

    referrals_text = ""
    referrals = await Database.get_referrals(user_id)
    if referrals:
        ref_list = "\n".join([f"• {r['referred_id']} – {datetime.fromtimestamp(r['created_at']).strftime('%d.%m.%Y')} (комиссия: {r['payment_amount']} {r['currency']})" for r in referrals])
        referrals_text = f"\n\n📊 *Рефералы:*\n{ref_list}"

    balance_ton = await Database.get_balance(user_id, 'TON')
    balance_stars = await Database.get_balance(user_id, 'STARS')
    balance_rub = await Database.get_balance(user_id, 'RUB')
    balance_text = f"\n\n💰 *Баланс:*\n• TON: {balance_ton:.2f}\n• ⭐️ Stars: {balance_stars:.2f}\n• RUB: {balance_rub:.2f}"
    
    text = (
        f"👤 *Профиль пользователя {user_id}*\n\n"
        f"🎭 Роль: {user[5]}\n\n"
        f"📅 *Статус подписки:*\n{sub_status}\n\n"
        f"🔧 *Фильтры:*\n{filters_text}"
        f"{referrals_text}"
        f"{balance_text}"
    )
    
    await update.message.reply_text(text, parse_mode='Markdown')

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Введите текст рассылки.")
        return
    text = ' '.join(context.args)
    context.user_data['broadcast_text'] = text
    keyboard = [
        [InlineKeyboardButton("✅ Подтвердить", callback_data='bc_confirm')],
        [InlineKeyboardButton("❌ Отмена", callback_data='bc_cancel')]
    ]
    await update.message.reply_text(f"Текст рассылки:\n\n{text}\n\nПодтвердите отправку всем пользователям?", reply_markup=InlineKeyboardMarkup(keyboard))

async def broadcast_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == 'bc_confirm':
        text = context.user_data.get('broadcast_text', '')
        if not text:
            await q.edit_message_text("Ошибка: текст не найден.")
            return
        await q.edit_message_text("Рассылка началась... Это может занять некоторое время.")
        users = await Database.get_all_users(limit=10000, offset=0)
        sent = 0
        failed = 0
        for user_id, _, _, _ in users:
            try:
                await context.bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown')
                sent += 1
                await asyncio.sleep(0.05)
            except Exception as e:
                logger.error(f"Ошибка рассылки {user_id}: {e}")
                failed += 1
        await q.message.reply_text(f"Рассылка завершена. Успешно: {sent}, ошибок: {failed}")
    else:
        await q.edit_message_text("Рассылка отменена.")
    context.user_data.pop('broadcast_text', None)

async def broadcast_mods(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Введите текст рассылки.")
        return
    text = ' '.join(context.args)
    mods = await Database.get_moderators()
    sent = 0
    for mod in mods:
        try:
            await context.bot.send_message(chat_id=mod['user_id'], text=text, parse_mode='Markdown')
            sent += 1
            await asyncio.sleep(0.05)
        except:
            pass
    await update.message.reply_text(f"Рассылка модераторам завершена. Отправлено: {sent}")

async def test_parse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("Запускаю тестовый парсинг...")
    ads = await fetch_all_ads()
    if ads:
        text = f"Найдено {len(ads)} объявлений. Первое:\n{ads[0].title}\n{ads[0].link}"
    else:
        text = "Объявлений не найдено."
    await update.message.reply_text(text)

async def daily_by_metro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    # Заглушка - можно реализовать позже
    await update.message.reply_text("Функция в разработке.")

async def admin_active_subs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rows = await Database.get_active_subscribers_detailed()
    if not rows:
        await update.message.reply_text("Нет активных подписчиков.")
        return
    text = "**Активные подписчики:**\n\n"
    for row in rows:
        user_id = row['user_id']
        until = row['subscribed_until']
        plan = row['plan'] or '—'
        source = row['subscription_source'] or 'unknown'
        remaining = (until - int(time.time())) // 86400
        text += f"• `{user_id}` | {plan} | осталось {remaining} дн. | источник: {source}\n"
    await update.message.reply_text(text, parse_mode='Markdown')

async def add_mod_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /add_mod <user_id>")
        return
    if not await check_user_exists(context.bot, user_id):
        await update.message.reply_text("❌ Пользователь с таким ID не найден в Telegram.")
        return
    perms = ['view_tickets', 'view_stats']
    await Database.add_moderator(user_id, perms, ADMIN_ID)
    
    try:
        await context.bot.send_message(
            chat_id=user_id,
            text=(
                "🎉 *Поздравляем! Вы стали модератором.*\n\n"
                "Теперь вам доступны следующие команды:\n"
                "• `/mod` – панель модератора\n"
                "• `/tickets` – список открытых тикетов\n"
                "• `/reply <id> <текст>` – ответить пользователю\n"
                "• `/close_ticket <id>` – закрыть тикет\n"
                "• `/view_ticket <id>` – история переписки по тикету\n\n"
                "Пожалуйста, используйте свои права ответственно."
            ),
            parse_mode='Markdown'
        )
    except Exception as e:
        logger.error(f"Не удалось уведомить нового модератора {user_id}: {e}")
    
    await update.message.reply_text(f"✅ Модератор {user_id} добавлен.")

async def remove_mod_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /remove_mod <user_id>")
        return
    await Database.remove_moderator(user_id)
    await update.message.reply_text(f"✅ Модератор {user_id} удалён.")

async def mods_list_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    mods = await Database.get_moderators()
    if not mods:
        await update.message.reply_text("Нет модераторов.")
        return
    text = "**Модераторы:**\n\n"
    for m in mods:
        text += f"• `{m['user_id']}` | права: {', '.join(m['permissions'])}\n"
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

# ---------- БАН / РАЗБАН ----------
async def ban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
    except:
        await update.message.reply_text("Использование: /ban <user_id>")
        return
    await Database.ban_user(user_id)
    await update.message.reply_text(f"Пользователь {user_id} забанен.")

async def unban_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
    except:
        await update.message.reply_text("Использование: /unban <user_id>")
        return
    await Database.unban_user(user_id)
    await update.message.reply_text(f"Пользователь {user_id} разбанен.")

# ---------- УПРАВЛЕНИЕ БАЛАНСОМ ----------
async def set_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        currency = context.args[1].upper()
        amount = float(context.args[2])
    except:
        await update.message.reply_text("Использование: /set_balance <user_id> <currency> <amount>")
        return
    # Обнуляем текущий баланс
    current = await Database.get_balance(user_id, currency)
    if current > 0:
        await Database.deduct_from_balance(user_id, currency, current)
    await Database.add_to_balance(user_id, currency, amount)
    await update.message.reply_text(f"Баланс {user_id} в {currency} установлен на {amount}.")

async def add_balance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        currency = context.args[1].upper()
        amount = float(context.args[2])
    except:
        await update.message.reply_text("Использование: /add_balance <user_id> <currency> <amount>")
        return
    await Database.add_to_balance(user_id, currency, amount)
    await update.message.reply_text(f"Добавлено {amount} {currency} пользователю {user_id}.")

# ---------- ЭКСПОРТ ПОЛЬЗОВАТЕЛЕЙ ----------
async def export_users(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    users = await Database.get_all_users(limit=10000, offset=0)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['user_id', 'subscribed_until', 'plan', 'subscription_source', 'role', 'referrer_id'])
    for row in users:
        writer.writerow(row)
    output.seek(0)
    await update.message.reply_document(document=output.getvalue().encode(), filename='users.csv')

# ---------- ФОНОВЫЕ ЗАДАЧИ ----------
async def send_ad_to_user(bot, user_id: int, ad: Ad, telegram_semaphore):
    """Отправляет одно объявление пользователю с защитой от флуда"""
    async with telegram_semaphore:
        if await Database.was_ad_sent_to_user(user_id, ad.id):
            return
            
        owner_text = "Собственник" if ad.owner else "Агент"
        deal_text = "Продажа" if ad.deal_type == 'sale' else "Аренда"
        source_icon = "🏢" if ad.source == 'cian' else "📱"
        source_name = "ЦИАН" if ad.source == 'cian' else "Авито"
        
        text = (
            f"🔵 *Новое объявление ({source_icon} {source_name})*\n"
            f"🏷 {ad.title}\n"
            f"💰 Цена: {ad.price}\n"
            f"📍 Адрес: {ad.address}\n"
            f"🚇 Метро: {ad.metro}\n"
            f"🏢 Этаж: {ad.floor}\n"
            f"📏 Площадь: {ad.area}\n"
            f"🛏 Комнат: {ad.rooms}\n"
            f"👤 {owner_text} | {deal_text}\n"
            f"[🔗 Ссылка на объявление]({ad.link})"
        )
        
        if len(text) > 1024 and ad.photos:
            text = truncate_text(text, 1000)
        
        try:
            if ad.photos and len(ad.photos) > 0:
                media = []
                media.append(
                    InputMediaPhoto(
                        media=ad.photos[0],
                        caption=text,
                        parse_mode='Markdown'
                    )
                )
                for photo_url in ad.photos[1:5]:
                    if photo_url:
                        media.append(InputMediaPhoto(media=photo_url))
                
                await bot.send_media_group(chat_id=user_id, media=media)
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode='Markdown',
                    disable_web_page_preview=True
                )
            
            await Database.mark_ad_sent(user_id, ad.id)
            await asyncio.sleep(0.1)
            
        except Exception as e:
            logger.error(f"Ошибка отправки пользователю {user_id}: {e}")

def matches_filters(ad: Ad, filters_dict: dict) -> bool:
    """Проверяет, подходит ли объявление под фильтры"""
    districts = filters_dict.get('districts', [])
    rooms = filters_dict.get('rooms', [])
    metros = filters_dict.get('metros', [])
    owner_only = filters_dict.get('owner_only', False)
    deal_type = filters_dict.get('deal_type', 'sale')
    sources = filters_dict.get('sources', ['cian', 'avito'])

    if ad.source not in sources:
        return False

    if not districts and not rooms and not metros and not owner_only:
        return False

    if ad.deal_type != deal_type:
        return False

    if districts and ad.district_detected:
        if ad.district_detected not in districts:
            return False

    if metros and ad.metro != 'Не указано':
        ad_metro_clean = ad.metro.lower().replace('м.', '').strip()
        found = False
        for m in metros:
            if m.lower() in ad_metro_clean or ad_metro_clean in m.lower():
                found = True
                break
        if not found:
            return False

    if rooms:
        room_type = None
        rc = ad.rooms
        if rc == 'студия':
            room_type = 'Студия'
        elif rc == '1':
            room_type = '1-комнатная'
        elif rc == '2':
            room_type = '2-комнатная'
        elif rc == '3':
            room_type = '3-комнатная'
        elif rc == '4' or (rc.isdigit() and int(rc) >= 4):
            room_type = '4-комнатная+'
        if room_type not in rooms:
            return False

    if owner_only and not ad.owner:
        return False

    return True

async def collector_loop(app: Application):
    """Фоновая задача: сбор объявлений и рассылка"""
    telegram_semaphore = app.bot_data.get('telegram_semaphore')
    
    while True:
        try:
            logger.info("Запуск сбора объявлений со всех площадок")
            
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
                logger.info("Нет новых объявлений после проверки БД")
                await asyncio.sleep(PARSING_INTERVAL)
                continue

            logger.info(f"Найдено {len(new_ads)} новых объявлений, начинаем рассылку")

            sent_count = 0
            for ad in new_ads:
                tasks = []
                for user_id, filters_json in subscribers:
                    if not filters_json:
                        continue
                    filters = json.loads(filters_json) if filters_json else {}
                    if not filters.get('districts') and not filters.get('rooms') and not filters.get('metros') and not filters.get('owner_only'):
                        continue
                    
                    if await Database.was_ad_sent_to_user(user_id, ad.id):
                        continue
                        
                    if matches_filters(ad, filters):
                        tasks.append(send_ad_to_user(app.bot, user_id, ad, telegram_semaphore))
                        sent_count += 1
                
                if tasks:
                    await asyncio.gather(*tasks)
                    await asyncio.sleep(1)

            logger.info(f"Рассылка завершена. Отправлено {sent_count} уведомлений")
            
        except Exception as e:
            logger.error(f"Ошибка в collector_loop: {e}", exc_info=True)
        
        await asyncio.sleep(PARSING_INTERVAL)

async def update_checker_loop(app: Application):
    """Проверка обновлений в GitHub и автоматическое обновление, если включён режим отладки"""
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
                    stdout, stderr = await proc.communicate()
                    current_commit = stdout.decode().strip()
                    
                    if GITHUB_TOKEN:
                        await asyncio.create_subprocess_exec('git', 'fetch', 'origin', GITHUB_BRANCH, cwd='/opt/bot')
                        proc = await asyncio.create_subprocess_exec(
                            'git', 'rev-parse', f'origin/{GITHUB_BRANCH}',
                            cwd='/opt/bot',
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        )
                    else:
                        proc = await asyncio.create_subprocess_exec(
                            'git', 'ls-remote', 'origin', GITHUB_BRANCH,
                            cwd='/opt/bot',
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        )
                    stdout, stderr = await proc.communicate()
                    remote_output = stdout.decode().strip()
                    if GITHUB_TOKEN:
                        remote_commit = remote_output
                    else:
                        remote_commit = remote_output.split()[0] if remote_output else ''
                    
                    if remote_commit and remote_commit != current_commit:
                        logger.info(f"Найдено обновление: {current_commit[:8]} -> {remote_commit[:8]}. Загружаем...")
                        
                        pull_proc = await asyncio.create_subprocess_exec(
                            'git', 'pull', 'origin', GITHUB_BRANCH,
                            cwd='/opt/bot',
                            stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE
                        )
                        stdout, stderr = await pull_proc.communicate()
                        if pull_proc.returncode == 0:
                            logger.info("Код обновлён. Проверяем зависимости...")
                            diff_proc = await asyncio.create_subprocess_exec(
                                'git', 'diff', '--name-only', 'HEAD@{1}', 'HEAD',
                                cwd='/opt/bot',
                                stdout=asyncio.subprocess.PIPE,
                                stderr=asyncio.subprocess.PIPE
                            )
                            stdout, stderr = await diff_proc.communicate()
                            if 'requirements.txt' in stdout.decode():
                                logger.info("Обновляем зависимости...")
                                await asyncio.create_subprocess_exec(
                                    sys.executable, '-m', 'pip', 'install', '-r', 'requirements.txt',
                                    cwd='/opt/bot'
                                )
                            
                            logger.info("Перезапуск бота...")
                            await asyncio.create_subprocess_exec('systemctl', 'restart', 'bot.service')
                            sys.exit(0)
                        else:
                            logger.error(f"Ошибка git pull: {stderr.decode()}")
                except Exception as e:
                    logger.error(f"Ошибка при проверке обновлений: {e}")
        except Exception as e:
            logger.error(f"Ошибка в цикле обновлений: {e}")
        
        await asyncio.sleep(AUTO_UPDATE_CHECK_INTERVAL)
        