import os
import logging
import sqlite3
import json
import requests
from bs4 import BeautifulSoup
import schedule
import time
from threading import Thread
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes
)
from telegram.constants import ParseMode

# ========== НАСТРОЙКИ (из переменных окружения) ==========
TOKEN = os.environ.get('TOKEN')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 0))
CRYPTO_WALLET = os.environ.get('CRYPTO_WALLET', '')
WALLET_USERNAME = os.environ.get('WALLET_USERNAME', '@your_wallet_username')

if not TOKEN or not ADMIN_ID:
    raise ValueError("Задайте переменные окружения TOKEN и ADMIN_ID")

# Цены подписок (в рублях)
PRICES = {
    '1month': 300,
    '3months': 800,   # экономия 100 руб.
    '6months': 1500,  # экономия 300 руб.
    '12months': 2800  # экономия 800 руб.
}

# Города и направления (округа/районы)
CITIES = {
    'Москва': ['ЦАО', 'САО', 'СВАО', 'ВАО', 'ЮВАО', 'ЮАО', 'ЮЗАО', 'ЗАО', 'СЗАО'],
    'Санкт-Петербург': ['Адмиралтейский', 'Василеостровский', 'Выборгский', 'Калининский', 'Кировский', 'Колпинский', 'Красногвардейский', 'Красносельский', 'Кронштадтский', 'Курортный', 'Московский', 'Невский', 'Петроградский', 'Петродворцовый', 'Приморский', 'Пушкинский', 'Фрунзенский', 'Центральный'],
    'Новосибирск': ['Дзержинский', 'Железнодорожный', 'Заельцовский', 'Калининский', 'Кировский', 'Ленинский', 'Октябрьский', 'Первомайский', 'Советский', 'Центральный'],
    'Екатеринбург': ['Академический', 'Верх-Исетский', 'Железнодорожный', 'Кировский', 'Ленинский', 'Октябрьский', 'Орджоникидзевский', 'Чкаловский'],
    'Казань': ['Авиастроительный', 'Вахитовский', 'Кировский', 'Московский', 'Ново-Савиновский', 'Приволжский', 'Советский'],
    'Нижний Новгород': ['Автозаводский', 'Канавинский', 'Ленинский', 'Московский', 'Нижегородский', 'Приокский', 'Советский', 'Сормовский']
}

# ========== БАЗА ДАННЫХ ==========
conn = sqlite3.connect('subscriptions.db', check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    filters TEXT,                -- JSON с городом и направлением
    subscribed_until INTEGER,    -- timestamp окончания подписки
    last_ad_id TEXT              -- ID последнего отправленного объявления
)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount_rub INTEGER,
    method TEXT,                 -- 'crypto' или 'wallet'
    plan TEXT,                   -- '1month', '3months' и т.д.
    txid TEXT,
    status TEXT DEFAULT 'pending'
)''')
conn.commit()

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# ========== ФУНКЦИИ РАБОТЫ С БД ==========
def get_user(user_id):
    cursor.execute('SELECT filters, subscribed_until, last_ad_id FROM users WHERE user_id = ?', (user_id,))
    return cursor.fetchone()

def set_user_filters(user_id, filters):
    cursor.execute('INSERT OR REPLACE INTO users (user_id, filters) VALUES (?, ?)',
                   (user_id, json.dumps(filters)))
    conn.commit()

def activate_subscription(user_id, days):
    import time
    until = int(time.time()) + days * 86400
    cursor.execute('UPDATE users SET subscribed_until = ? WHERE user_id = ?', (until, user_id))
    conn.commit()

def is_subscribed(user_id):
    user = get_user(user_id)
    if user and user[1]:
        import time
        return user[1] > time.time()
    return False

def update_last_ad(user_id, ad_id):
    cursor.execute('UPDATE users SET last_ad_id = ? WHERE user_id = ?', (ad_id, user_id))
    conn.commit()

# ========== ОБРАБОТЧИКИ КОМАНД ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "👋 Привет! Я бот, который помогает находить **новые объявления о квартирах от собственников** на ЦИАН.\n\n"
        "🔍 Как только появляется свежее объявление, я сразу пришлю вам:\n"
        "• Ссылку на объявление\n"
        "• Цену, адрес, метро, этаж, площадь\n"
        "• Отметку, собственник или агент\n"
        "• Первые 3 фото\n\n"
        "Чтобы начать получать объявления, нужно оформить подписку и выбрать город и направление."
    )
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='choose_plan')],
        [InlineKeyboardButton("ℹ️ Мой статус", callback_data='my_status')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(welcome_text, parse_mode='Markdown', reply_markup=reply_markup)

async def my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = get_user(user_id)
    import time
    if user and user[1] and user[1] > time.time():
        remaining = user[1] - int(time.time())
        days = remaining // 86400
        hours = (remaining % 86400) // 3600
        status = f"✅ Подписка активна. Осталось: {days} дн. {hours} ч."
    else:
        status = "❌ Подписка не активна."
    filters = user[0] if user and user[0] else "не настроены"
    await update.message.reply_text(f"{status}\nВаши фильтры: {filters}")

# ---------- ВЫБОР ПЛАНА ПОДПИСКИ ----------
async def choose_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton(f"1 месяц – {PRICES['1month']} руб.", callback_data='plan_1month')],
        [InlineKeyboardButton(f"3 месяца – {PRICES['3months']} руб. (экономия 100 руб.)", callback_data='plan_3months')],
        [InlineKeyboardButton(f"6 месяцев – {PRICES['6months']} руб. (экономия 300 руб.)", callback_data='plan_6months')],
        [InlineKeyboardButton(f"12 месяцев – {PRICES['12months']} руб. (экономия 800 руб.)", callback_data='plan_12months')],
        [InlineKeyboardButton("« Назад", callback_data='back_to_start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Выберите срок подписки:", reply_markup=reply_markup)

async def plan_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    plan = query.data.split('_')[1]  # '1month', '3months' и т.д.
    context.user_data['plan'] = plan
    # Предлагаем способы оплаты
    keyboard = [
        [InlineKeyboardButton(f"₿ Криптовалюта (USDT)", callback_data='pay_crypto')],
        [InlineKeyboardButton(f"💳 Кошелёк Telegram (@wallet)", callback_data='pay_wallet')],
        [InlineKeyboardButton("« Назад", callback_data='choose_plan')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("Выберите способ оплаты:", reply_markup=reply_markup)

async def pay_crypto(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    plan = context.user_data.get('plan', '1month')
    amount = PRICES[plan]
    cursor.execute('INSERT INTO payments (user_id, amount_rub, method, plan) VALUES (?, ?, ?, ?)',
                   (user_id, amount, 'crypto', plan))
    conn.commit()
    payment_id = cursor.lastrowid
    text = (
        f"Оплата криптовалютой:\n\n"
        f"Сумма: {amount} руб. в эквиваленте USDT (TRC20)\n"
        f"Кошелёк для перевода:\n`{CRYPTO_WALLET}`\n\n"
        "После перевода **отправьте сюда TXID** транзакции (или скриншот).\n"
        "Администратор проверит и активирует подписку вручную.\n\n"
        f"ID платежа: `{payment_id}`"
    )
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)

async def pay_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    plan = context.user_data.get('plan', '1month')
    amount = PRICES[plan]
    cursor.execute('INSERT INTO payments (user_id, amount_rub, method, plan) VALUES (?, ?, ?, ?)',
                   (user_id, amount, 'wallet', plan))
    conn.commit()
    payment_id = cursor.lastrowid
    text = (
        f"Оплата через кошелёк Telegram (@wallet):\n\n"
        f"Сумма: {amount} руб.\n"
        f"Получатель: **{WALLET_USERNAME}**\n\n"
        "1. Откройте @wallet.\n"
        "2. Переведите указанную сумму на этот аккаунт.\n"
        "3. После перевода **отправьте сюда скриншот** или номер транзакции.\n\n"
        "Администратор проверит и активирует подписку вручную.\n\n"
        f"ID платежа: `{payment_id}`"
    )
    await query.edit_message_text(text, parse_mode=ParseMode.MARKDOWN)

async def handle_payment_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message = update.message
    if message.photo:
        caption = f"Пользователь {user_id} отправил скриншот оплаты."
        await context.bot.send_photo(chat_id=ADMIN_ID, photo=message.photo[-1].file_id, caption=caption)
        await message.reply_text("Скриншот отправлен администратору. Ожидайте подтверждения.")
    elif message.text:
        txid = message.text.strip()
        cursor.execute('UPDATE payments SET txid = ? WHERE user_id = ? AND status="pending"', (txid, user_id))
        conn.commit()
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"Пользователь {user_id} отправил подтверждение: {txid}\nДля активации подписки отправьте:\n/activate {user_id}"
        )
        await message.reply_text("Данные получены. Ожидайте подтверждения от администратора.")
    else:
        await message.reply_text("Пожалуйста, отправьте TXID или скриншот.")

# ---------- ВЫБОР ГОРОДА И НАПРАВЛЕНИЯ (после активации подписки) ----------
async def select_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Вызывается после активации подписки администратором, либо пользователь может сам зайти в настройки."""
    user_id = update.effective_user.id
    if not is_subscribed(user_id):
        await update.message.reply_text("Сначала оформите подписку.")
        return
    keyboard = []
    for city in CITIES.keys():
        keyboard.append([InlineKeyboardButton(city, callback_data=f'city_{city}')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Выберите город:", reply_markup=reply_markup)

async def city_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    city = query.data.split('_')[1]
    context.user_data['city'] = city
    # Показываем направления для выбранного города
    districts = CITIES[city]
    keyboard = []
    for d in districts:
        keyboard.append([InlineKeyboardButton(d, callback_data=f'district_{d}')])
    # Добавим кнопку "Пропустить" (если не хочет выбирать направление, но лучше обязать)
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(f"Выберите район/направление в городе {city}:", reply_markup=reply_markup)

async def district_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    district = query.data.split('_')[1]
    city = context.user_data.get('city')
    filters = {'city': city, 'district': district}
    set_user_filters(query.from_user.id, filters)
    await query.edit_message_text(f"✅ Фильтры сохранены: город {city}, район {district}. Теперь вы будете получать объявления.")

# ---------- АДМИНСКАЯ КОМАНДА ----------
async def activate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        # Находим платёж с этим user_id
        cursor.execute('SELECT plan FROM payments WHERE user_id = ? AND status="pending" ORDER BY id DESC LIMIT 1', (user_id,))
        row = cursor.fetchone()
        if row:
            plan = row[0]
            days = {'1month': 30, '3months': 90, '6months': 180, '12months': 360}[plan]
            activate_subscription(user_id, days)
            cursor.execute('UPDATE payments SET status="confirmed" WHERE user_id=? AND status="pending"', (user_id,))
            conn.commit()
            await update.message.reply_text(f"Подписка для {user_id} активирована на {days} дней.")
            # Уведомляем пользователя и предлагаем выбрать город
            await context.bot.send_message(chat_id=user_id, text="✅ Ваша подписка активирована! Теперь выберите город и район для получения объявлений.")
            # Можно отправить клавиатуру выбора города
            await select_city(update, context)  # но это не сработает, т.к. update от админа. Лучше отправить отдельное сообщение с кнопками.
            # Сделаем так:
            keyboard = []
            for city in CITIES.keys():
                keyboard.append([InlineKeyboardButton(city, callback_data=f'city_{city}')])
            reply_markup = InlineKeyboardMarkup(keyboard)
            await context.bot.send_message(chat_id=user_id, text="Выберите город:", reply_markup=reply_markup)
        else:
            await update.message.reply_text("Не найдено ожидающих платежей для этого пользователя.")
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /activate user_id")

async def back_to_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await start(query, context)

# ========== ПАРСИНГ И РАССЫЛКА ==========
def fetch_cian(city, district):
    """
    Функция парсинга ЦИАН.
    Требует адаптации под реальную структуру сайта.
    Возвращает список объявлений: [{'id': str, 'title': str, 'link': str, 'price': str, 'address': str, 'metro': str, 'floor': str, 'area': str, 'rooms': str, 'owner': bool, 'photos': [url1, url2, url3]}]
    """
    # ВНИМАНИЕ: Это пример. Необходимо заменить URL, параметры и селекторы.
    # Для Москвы округа могут кодироваться по-разному. Упростим: будем искать по ключевому слову в адресе? Но лучше через параметры.
    # В реальности нужно исследовать запросы сайта.
    # Для демонстрации вернём тестовые данные.
    # Позже вы должны будете заменить этот код на реальный парсинг.
    
    # Заглушка: возвращаем тестовое объявление, если его ещё нет в БД.
    # В реальности здесь должен быть запрос к ЦИАН и разбор HTML.
    
    # Пример тестовых данных
    test_ad = {
        'id': '123456',
        'title': 'Продаётся 2-комнатная квартира',
        'link': 'https://cian.ru/sale/flat/123456/',
        'price': '12 500 000 ₽',
        'address': 'ул. Примерная, д. 10',
        'metro': 'м. Комсомольская (10 мин пешком)',
        'floor': '5/9',
        'area': '55 м²',
        'rooms': '2',
        'owner': True,
        'photos': [
            'https://example.com/photo1.jpg',
            'https://example.com/photo2.jpg',
            'https://example.com/photo3.jpg'
        ]
    }
    return [test_ad]

def check_new_ads():
    import time
    now = int(time.time())
    cursor.execute('SELECT user_id, filters FROM users WHERE subscribed_until > ?', (now,))
    users = cursor.fetchall()
    for user_id, filters_json in users:
        filters = json.loads(filters_json)
        city = filters.get('city')
        district = filters.get('district')
        if not city or not district:
            continue
        ads = fetch_cian(city, district)
        if not ads:
            continue
        cursor.execute('SELECT last_ad_id FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        last_ad_id = row[0] if row else None
        new_ads = [ad for ad in ads if ad['id'] != last_ad_id]
        for ad in new_ads[:1]:  # пока отправляем только первое новое, чтобы не спамить
            # Формируем сообщение
            owner_text = "Собственник" if ad.get('owner') else "Агент"
            text = (
                f"🔵 *Новое объявление*\n"
                f"{ad['title']}\n"
                f"💰 Цена: {ad['price']}\n"
                f"📍 Адрес: {ad['address']}\n"
                f"🚇 Метро: {ad['metro']}\n"
                f"🏢 Этаж: {ad['floor']}\n"
                f"📏 Площадь: {ad['area']}\n"
                f"🛏 Комнат: {ad['rooms']}\n"
                f"👤 {owner_text}\n"
                f"[Ссылка на объявление]({ad['link']})"
            )
            # Отправляем сообщение
            try:
                from telegram import Bot
                bot = Bot(TOKEN)
                # Отправляем текст
                bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown', disable_web_page_preview=True)
                # Отправляем до 3 фото
                for photo_url in ad.get('photos', [])[:3]:
                    try:
                        bot.send_photo(chat_id=user_id, photo=photo_url)
                    except:
                        pass
                update_last_ad(user_id, ad['id'])
            except Exception as e:
                logger.error(f"Ошибка отправки {user_id}: {e}")

def run_schedule():
    schedule.every(10).minutes.do(check_new_ads)
    while True:
        schedule.run_pending()
        time.sleep(1)

# ========== ЗАПУСК БОТА ==========
def main():
    Thread(target=run_schedule, daemon=True).start()

    application = Application.builder().token(TOKEN).build()

    application.add_handler(CommandHandler('start', start))
    application.add_handler(CallbackQueryHandler(my_status, pattern='^my_status$'))
    application.add_handler(CallbackQueryHandler(choose_plan, pattern='^choose_plan$'))
    application.add_handler(CallbackQueryHandler(plan_chosen, pattern='^plan_'))
    application.add_handler(CallbackQueryHandler(pay_crypto, pattern='^pay_crypto$'))
    application.add_handler(CallbackQueryHandler(pay_wallet, pattern='^pay_wallet$'))
    application.add_handler(CallbackQueryHandler(back_to_start, pattern='^back_to_start$'))
    application.add_handler(CallbackQueryHandler(city_chosen, pattern='^city_'))
    application.add_handler(CallbackQueryHandler(district_chosen, pattern='^district_'))
    application.add_handler(MessageHandler(filters.PHOTO, handle_payment_proof))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_payment_proof))
    application.add_handler(CommandHandler('activate', activate_command))

    application.run_polling()

if __name__ == '__main__':
    main()
