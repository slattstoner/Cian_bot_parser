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
TON_WALLET = os.environ.get('TON_WALLET', '')  # адрес вашего TON кошелька
DADATA_API_KEY = os.environ.get('DADATA_API_KEY', '')

if not TOKEN or not ADMIN_ID:
    raise ValueError("Задайте переменные окружения TOKEN и ADMIN_ID")
if not TON_WALLET:
    raise ValueError("Задайте переменную TON_WALLET")

# Цены подписок в TON (фиксированные, не зависят от курса)
PRICES_TON = {
    '1month': 1.5,
    '3months': 4.0,    # скидка
    '6months': 7.5,    # скидка
    '12months': 14.0   # скидка
}

# Для обратной совместимости с другими частями кода (не используется в платежах)
PRICES = {k: int(v * 100) for k, v in PRICES_TON.items()}  # заглушка

# ========== ГОРОДА И ИХ СТАНЦИИ МЕТРО ==========
METRO_STATIONS = {
    'Москва': [
        'Комсомольская', 'Красные ворота', 'Чистые пруды', 'Лубянка', 'Охотный ряд',
        'Библиотека им. Ленина', 'Кропоткинская', 'Парк культуры', 'Фрунзенская',
        'Спортивная', 'Воробьёвы горы', 'Университет', 'Проспект Вернадского',
        'Юго-Западная', 'Тропарёво', 'Румянцево', 'Саларьево', 'Полежаевская',
        'Щукинская', 'Строгино', 'Крылатское', 'Молодёжная', 'Кунцевская',
        'Славянский бульвар', 'Парк Победы', 'Кутузовская', 'Студенческая',
        'Международная', 'Выставочная', 'Киевская', 'Смоленская', 'Арбатская',
        'Александровский сад', 'Боровицкая', 'Полянка', 'Третьяковская',
        'Новокузнецкая', 'Таганская', 'Марксистская', 'Пролетарская',
        'Волгоградский проспект', 'Текстильщики', 'Кузьминки', 'Рязанский проспект',
        'Выхино', 'Лермонтовский проспект', 'Жулебино', 'Котельники'
    ],
    'Санкт-Петербург': [
        'Адмиралтейская', 'Василеостровская', 'Гостиный двор', 'Маяковская',
        'Площадь Восстания', 'Владимирская', 'Пушкинская', 'Технологический институт',
        'Балтийская', 'Нарвская', 'Кировский завод', 'Автово', 'Ленинский проспект',
        'Проспект Ветеранов', 'Девяткино', 'Гражданский проспект', 'Академическая',
        'Политехническая', 'Площадь Мужества', 'Лесная', 'Выборгская',
        'Площадь Ленина', 'Чернышевская', 'Невский проспект', 'Сенная площадь',
        'Спасская', 'Достоевская', 'Лиговский проспект', 'Площадь Александра Невского',
        'Новочеркасская', 'Ладожская', 'Проспект Большевиков', 'Улица Дыбенко'
    ],
    'Новосибирск': [
        'Площадь Ленина', 'Красный проспект', 'Гагаринская', 'Заельцовская',
        'Октябрьская', 'Речной вокзал', 'Студенческая', 'Площадь Маркса'
    ],
    'Екатеринбург': [
        'Проспект Космонавтов', 'Уралмаш', 'Машиностроителей', 'Уральская',
        'Динамо', 'Площадь 1905 года', 'Геологическая', 'Чкаловская',
        'Ботаническая'
    ],
    'Казань': [
        'Кремлёвская', 'Площадь Тукая', 'Суконная слобода', 'Аметьево',
        'Горки', 'Проспект Победы'
    ],
    'Нижний Новгород': [
        'Московская', 'Чкаловская', 'Ленинская', 'Заречная', 'Двигатель Революции',
        'Пролетарская', 'Автозаводская', 'Комсомольская', 'Парк культуры'
    ]
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

# Соответствие полных названий округов сокращениям (для Москвы)
DISTRICT_MAPPING = {
    "Центральный административный округ": "ЦАО",
    "Северный административный округ": "САО",
    "Северо-Восточный административный округ": "СВАО",
    "Восточный административный округ": "ВАО",
    "Юго-Восточный административный округ": "ЮВАО",
    "Южный административный округ": "ЮАО",
    "Юго-Западный административный округ": "ЮЗАО",
    "Западный административный округ": "ЗАО",
    "Северо-Западный административный округ": "СЗАО"
}

# ========== БАЗА ДАННЫХ ==========
conn = sqlite3.connect('subscriptions.db', timeout=10, check_same_thread=False)
cursor = conn.cursor()
cursor.execute('''CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    filters TEXT,
    subscribed_until INTEGER,
    last_ad_id TEXT
)''')
cursor.execute('''CREATE TABLE IF NOT EXISTS payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER,
    amount_ton REAL,
    plan TEXT,
    txid TEXT,
    status TEXT DEFAULT 'pending'
)''')
conn.commit()

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
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

# ========== ГЕОКОДИНГ (определение округа по адресу) ==========
def get_district_by_address(address):
    if not DADATA_API_KEY:
        return None
    url = "https://dadata.ru/api/v2/clean/address"
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Token {DADATA_API_KEY}"
    }
    data = [address]
    try:
        r = requests.post(url, headers=headers, json=data, timeout=5)
        r.raise_for_status()
        result = r.json()[0]
        area_type = result.get('area_type')
        area = result.get('area')
        if area_type == "округ" and area:
            return DISTRICT_MAPPING.get(area)
        return None
    except Exception as e:
        logger.error(f"Ошибка геокодирования '{address}': {e}")
        return None

# ========== ОБРАБОТЧИКИ КОМАНД ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "👋 Добро пожаловать в **Realty Parser Bot**!\n\n"
        "🔍 Я отслеживаю **новые объявления о квартирах от собственников** на ЦИАН и присылаю их вам сразу после публикации.\n\n"
        "📦 В каждом сообщении:\n"
        "• Ссылка на объявление\n"
        "• Цена, адрес, метро, этаж, площадь\n"
        "• Отметка: собственник или агент\n"
        "• Первые 3 фото\n\n"
        "⚙️ Чтобы начать, оформите подписку и настройте фильтры.\n\n"
        "💎 Оплата принимается в **TON** (криптовалюта)."
    )
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='choose_plan')],
        [InlineKeyboardButton("ℹ️ Мой статус", callback_data='my_status')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='start_filter')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(welcome_text, parse_mode='Markdown', reply_markup=reply_markup)

async def my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает статус подписки и текущие фильтры."""
    if update.callback_query:
        user_id = update.callback_query.from_user.id
        message = update.callback_query.message
        send_func = message.reply_text
    else:
        user_id = update.effective_user.id
        send_func = update.message.reply_text

    user = get_user(user_id)
    import time
    now = int(time.time())

    if user and user[1] and user[1] > now:
        remaining = user[1] - now
        days = remaining // 86400
        hours = (remaining % 86400) // 3600
        status = f"✅ **Подписка активна**\nОсталось: {days} дн. {hours} ч."
    else:
        status = "❌ **Подписка не активна**"

    filters_raw = user[0] if user and user[0] else None
    if filters_raw:
        try:
            filters = json.loads(filters_raw)
            city = filters.get('city', '?')
            districts = filters.get('districts', [])
            metros = filters.get('metros', [])
            districts_str = ', '.join(districts) if districts else 'все'
            metros_str = ', '.join(metros) if metros else 'все'
            filters_display = f"🏙 **Город:** {city}\n🏘 **Округа:** {districts_str}\n🚇 **Метро:** {metros_str}"
        except:
            filters_display = "⚠️ Ошибка в формате фильтров"
    else:
        filters_display = "⚙️ Фильтры не настроены"

    await send_func(f"{status}\n\n{filters_display}", parse_mode='Markdown')

# ---------- НАСТРОЙКА ФИЛЬТРОВ ----------
async def start_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = []
    for city in CITIES.keys():
        keyboard.append([InlineKeyboardButton(city, callback_data=f'filter_city_{city}')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("🏙 Выберите город:", reply_markup=reply_markup)

async def filter_city_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    city = query.data.split('_')[2]
    context.user_data['filter_city'] = city
    context.user_data['selected_districts'] = []
    context.user_data['selected_metros'] = []

    districts = CITIES[city]
    keyboard = []
    for d in districts:
        keyboard.append([InlineKeyboardButton(f"⬜️ {d}", callback_data=f'toggle_district_{d}')])
    keyboard.append([InlineKeyboardButton("✅ Готово (округа)", callback_data='filter_districts_done')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"🏘 Выберите **один или несколько округов** в городе {city} (после выбора нажмите Готово):",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def toggle_district(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    district = query.data.split('_')[2]
    selected = context.user_data.get('selected_districts', [])
    if district in selected:
        selected.remove(district)
    else:
        selected.append(district)
    context.user_data['selected_districts'] = selected

    city = context.user_data['filter_city']
    districts = CITIES[city]
    keyboard = []
    for d in districts:
        if d in selected:
            keyboard.append([InlineKeyboardButton(f"✅ {d}", callback_data=f'toggle_district_{d}')])
        else:
            keyboard.append([InlineKeyboardButton(f"⬜️ {d}", callback_data=f'toggle_district_{d}')])
    keyboard.append([InlineKeyboardButton("✅ Готово (округа)", callback_data='filter_districts_done')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"🏘 Выберите округа в городе {city} (отмеченные ✅ будут добавлены):",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def filter_districts_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    city = context.user_data['filter_city']
    metros = METRO_STATIONS.get(city, [])
    if not metros:
        await save_filters_and_finish(update, context)
        return

    context.user_data['selected_metros'] = []
    keyboard = []
    for m in metros[:30]:
        keyboard.append([InlineKeyboardButton(f"⬜️ {m}", callback_data=f'toggle_metro_{m}')])
    keyboard.append([InlineKeyboardButton("✅ Готово (метро)", callback_data='filter_metros_done')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"🚇 Выберите **одну или несколько станций метро** в городе {city}:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def toggle_metro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    metro = query.data.split('_')[2]
    selected = context.user_data.get('selected_metros', [])
    if metro in selected:
        selected.remove(metro)
    else:
        selected.append(metro)
    context.user_data['selected_metros'] = selected

    city = context.user_data['filter_city']
    metros = METRO_STATIONS.get(city, [])
    keyboard = []
    for m in metros[:30]:
        if m in selected:
            keyboard.append([InlineKeyboardButton(f"✅ {m}", callback_data=f'toggle_metro_{m}')])
        else:
            keyboard.append([InlineKeyboardButton(f"⬜️ {m}", callback_data=f'toggle_metro_{m}')])
    keyboard.append([InlineKeyboardButton("✅ Готово (метро)", callback_data='filter_metros_done')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"🚇 Выберите станции метро в городе {city}:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def filter_metros_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await save_filters_and_finish(update, context)

async def save_filters_and_finish(update, context):
    query = update.callback_query
    user_id = query.from_user.id
    city = context.user_data.get('filter_city')
    districts = context.user_data.get('selected_districts', [])
    metros = context.user_data.get('selected_metros', [])

    filters = {
        'city': city,
        'districts': districts,
        'metros': metros
    }
    set_user_filters(user_id, filters)

    text = f"✅ **Фильтры сохранены!**\n\n🏙 Город: {city}\n"
    if districts:
        text += f"🏘 Округа: {', '.join(districts)}\n"
    else:
        text += f"🏘 Округа: не выбраны (будут приходить все)\n"
    if metros:
        text += f"🚇 Метро: {', '.join(metros)}\n"
    else:
        text += f"🚇 Метро: не выбраны (будут приходить все)\n"
    text += "\nТеперь вы будете получать объявления, подходящие хотя бы под один из выбранных фильтров."

    await query.edit_message_text(text, parse_mode='Markdown')

    # Возвращаем в главное меню
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='choose_plan')],
        [InlineKeyboardButton("ℹ️ Мой статус", callback_data='my_status')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='start_filter')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await context.bot.send_message(chat_id=user_id, text="Главное меню:", reply_markup=reply_markup)

# ---------- ВЫБОР ПЛАНА ПОДПИСКИ (только TON) ----------
async def choose_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton(f"1 месяц – {PRICES_TON['1month']} TON", callback_data='plan_1month')],
        [InlineKeyboardButton(f"3 месяца – {PRICES_TON['3months']} TON (экономия)", callback_data='plan_3months')],
        [InlineKeyboardButton(f"6 месяцев – {PRICES_TON['6months']} TON (экономия)", callback_data='plan_6months')],
        [InlineKeyboardButton(f"12 месяцев – {PRICES_TON['12months']} TON (экономия)", callback_data='plan_12months')],
        [InlineKeyboardButton("« Назад", callback_data='back_to_start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("📅 Выберите срок подписки:", reply_markup=reply_markup)

async def plan_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    plan = query.data.split('_')[1]  # '1month', '3months'...
    context.user_data['plan'] = plan
    # Единственный способ оплаты - TON
    await pay_ton(update, context)

async def pay_ton(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    plan = context.user_data.get('plan', '1month')
    amount_ton = PRICES_TON[plan]
    cursor.execute('INSERT INTO payments (user_id, amount_ton, plan) VALUES (?, ?, ?)',
                   (user_id, amount_ton, plan))
    conn.commit()
    payment_id = cursor.lastrowid
    text = (
        f"**Оплата в TON**\n\n"
        f"Сумма: **{amount_ton} TON**\n"
        f"Кошелёк для перевода:\n`{TON_WALLET}`\n\n"
        "После перевода **отправьте сюда TXID** транзакции (или скриншот).\n"
        "Администратор проверит и активирует подписку вручную.\n\n"
        f"**ID платежа:** `{payment_id}`"
    )
    await query.edit_message_text(text, parse_mode='Markdown')

async def handle_payment_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    message = update.message
    if message.photo:
        caption = f"Пользователь {user_id} отправил скриншот оплаты TON."
        await context.bot.send_photo(chat_id=ADMIN_ID, photo=message.photo[-1].file_id, caption=caption)
        await message.reply_text("✅ Скриншот отправлен администратору. Ожидайте подтверждения.")
    elif message.text:
        txid = message.text.strip()
        cursor.execute('UPDATE payments SET txid = ? WHERE user_id = ? AND status="pending"', (txid, user_id))
        conn.commit()
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"Пользователь {user_id} отправил подтверждение TON: {txid}\nДля активации подписки отправьте:\n/activate {user_id}"
        )
        await message.reply_text("✅ Данные получены. Ожидайте подтверждения от администратора.")
    else:
        await message.reply_text("Пожалуйста, отправьте TXID или скриншот.")

async def back_to_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='choose_plan')],
        [InlineKeyboardButton("ℹ️ Мой статус", callback_data='my_status')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='start_filter')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    welcome_text = (
        "👋 Добро пожаловать в **Realty Parser Bot**!\n\n"
        "🔍 Я отслеживаю **новые объявления о квартирах от собственников** на ЦИАН и присылаю их вам сразу после публикации.\n\n"
        "📦 В каждом сообщении:\n"
        "• Ссылка на объявление\n"
        "• Цена, адрес, метро, этаж, площадь\n"
        "• Отметка: собственник или агент\n"
        "• Первые 3 фото\n\n"
        "⚙️ Чтобы начать, оформите подписку и настройте фильтры.\n\n"
        "💎 Оплата принимается в **TON**."
    )
    await query.edit_message_text(welcome_text, parse_mode='Markdown', reply_markup=reply_markup)

# ---------- АДМИНСКИЕ КОМАНДЫ ----------
async def activate_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        cursor.execute('SELECT plan FROM payments WHERE user_id = ? AND status="pending" ORDER BY id DESC LIMIT 1', (user_id,))
        row = cursor.fetchone()
        if row:
            plan = row[0]
            days = {'1month': 30, '3months': 90, '6months': 180, '12months': 360}[plan]
            activate_subscription(user_id, days)
            cursor.execute('UPDATE payments SET status="confirmed" WHERE user_id=? AND status="pending"', (user_id,))
            conn.commit()
            await update.message.reply_text(f"✅ Подписка для {user_id} активирована на {days} дней.")
            await context.bot.send_message(
                chat_id=user_id,
                text="✅ Ваша подписка активирована! Теперь настройте фильтры в главном меню."
            )
        else:
            await update.message.reply_text("❌ Не найдено ожидающих платежей для этого пользователя.")
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /activate user_id")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def grant_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        days = int(context.args[1])
        activate_subscription(user_id, days)
        await update.message.reply_text(f"✅ Подписка для пользователя {user_id} активирована на {days} дней.")
        await context.bot.send_message(
            chat_id=user_id,
            text=f"✅ Администратор выдал вам подписку на {days} дней! Настройте фильтры в главном меню."
        )
    except (IndexError, ValueError):
        await update.message.reply_text("❌ Использование: /grant user_id days")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    cursor.execute("SELECT COUNT(*) FROM users")
    total_users = cursor.fetchone()[0]
    import time
    now = int(time.time())
    cursor.execute("SELECT COUNT(*) FROM users WHERE subscribed_until > ?", (now,))
    active_subs = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM payments WHERE status='pending'")
    pending_payments = cursor.fetchone()[0]
    text = (
        f"📊 **Статистика бота**\n"
        f"👥 Всего пользователей: {total_users}\n"
        f"✅ Активных подписок: {active_subs}\n"
        f"⏳ Ожидающих платежей: {pending_payments}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')

async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    cursor.execute("SELECT user_id, subscribed_until FROM users ORDER BY user_id LIMIT 20")
    rows = cursor.fetchall()
    if not rows:
        await update.message.reply_text("Нет пользователей.")
        return
    text = "**Список пользователей (первые 20):**\n"
    import time
    now = int(time.time())
    for user_id, until in rows:
        if until and until > now:
            remaining = (until - now) // 86400
            status = f"✅ активна (осталось {remaining} дн.)"
        else:
            status = "❌ не активна"
        text += f"• `{user_id}` — {status}\n"
    await update.message.reply_text(text, parse_mode='Markdown')

async def find_user_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        user = get_user(user_id)
        if not user:
            await update.message.reply_text(f"Пользователь {user_id} не найден.")
            return
        filters_json, subscribed_until, last_ad_id = user
        import time
        now = int(time.time())
        if subscribed_until and subscribed_until > now:
            remaining = (subscribed_until - now) // 86400
            status = f"✅ активна (осталось {remaining} дн.)"
        else:
            status = "❌ не активна"
        filters = json.loads(filters_json) if filters_json else "не настроены"
        text = (
            f"**Информация о пользователе {user_id}**\n"
            f"Статус подписки: {status}\n"
            f"Фильтры: {filters}\n"
            f"Последнее отправленное объявление: {last_ad_id or 'нет'}"
        )
        await update.message.reply_text(text, parse_mode='Markdown')
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /find user_id")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def broadcast_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Укажите текст для рассылки.\nПример: /broadcast Всем привет!")
        return
    text = ' '.join(context.args)
    keyboard = [
        [InlineKeyboardButton("✅ Да, отправить", callback_data='broadcast_confirm')],
        [InlineKeyboardButton("❌ Отмена", callback_data='broadcast_cancel')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    context.user_data['broadcast_text'] = text
    await update.message.reply_text(
        f"Вы хотите отправить это сообщение **ВСЕМ** пользователям?\n\n{text}",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def broadcast_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        return
    text = context.user_data.get('broadcast_text', '')
    if not text:
        await query.edit_message_text("Ошибка: текст не найден.")
        return
    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()
    success = 0
    failed = 0
    for (user_id,) in users:
        try:
            await context.bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown')
            success += 1
        except Exception as e:
            logger.error(f"Ошибка отправки {user_id}: {e}")
            failed += 1
    await query.edit_message_text(f"✅ Рассылка завершена.\nУспешно: {success}\nОшибок: {failed}")

async def broadcast_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    if query.from_user.id != ADMIN_ID:
        return
    await query.edit_message_text("Рассылка отменена.")

# ========== ПАРСИНГ И РАССЫЛКА (реальный cianparser) ==========
def fetch_cian(city, districts, metros):
    import cianparser
    import logging
    logger = logging.getLogger(__name__)

    try:
        parser = cianparser.CianParser(location=city)
    except Exception as e:
        logger.error(f"Не удалось создать парсер для города {city}: {e}")
        return []

    settings = {
        "start_page": 1,
        "end_page": 1,
        "is_by_homeowner": True,
        "sort_by": "creation_data_from_newer_to_older",
    }

    try:
        raw_data = parser.get_flats(
            deal_type="sale",
            rooms="all",
            with_extra_data=True,
            additional_settings=settings
        )
    except Exception as e:
        logger.error(f"Ошибка при парсинге: {e}")
        return []

    formatted_ads = []
    for item in raw_data:
        if item.get('accommodation_type') != 'flat':
            continue

        detected_district = item.get('district')
        if detected_district and city == 'Москва':
            detected_district = DISTRICT_MAPPING.get(detected_district)

        detected_metro = item.get('underground')

        district_ok = False
        metro_ok = False
        if districts and detected_district and detected_district in districts:
            district_ok = True
        if metros and detected_metro and detected_metro in metros:
            metro_ok = True

        if (not districts and not metros) or district_ok or metro_ok:
            address_parts = []
            if item.get('street'):
                address_parts.append(f"ул. {item['street']}")
            if item.get('house_number'):
                address_parts.append(f"д. {item['house_number']}")
            full_address = f"{city}, {' '.join(address_parts)}" if address_parts else city

            author_type = item.get('author_type')
            is_owner = (author_type == 'owner')

            link = item.get('url', f"https://cian.ru/sale/flat/{item.get('id', '')}/")

            ad = {
                'id': str(item.get('id', '')),
                'title': f"{item.get('rooms_count', '?')}-к. квартира",
                'link': link,
                'price': f"{item.get('price', 0):,} ₽".replace(',', ' '),
                'address': full_address,
                'metro': detected_metro or 'Не указано',
                'floor': f"{item.get('floor', '?')}/{item.get('floors_count', '?')}",
                'area': f"{item.get('total_meters', 0)} м²",
                'rooms': str(item.get('rooms_count', '?')),
                'owner': is_owner,
                'photos': [],
                'district_detected': detected_district
            }
            formatted_ads.append(ad)

    logger.info(f"Найдено {len(formatted_ads)} новых объявлений в {city} (после фильтрации)")
    return formatted_ads

def check_new_ads():
    import time
    now = int(time.time())
    cursor.execute('SELECT user_id, filters FROM users WHERE subscribed_until > ?', (now,))
    users = cursor.fetchall()
    for user_id, filters_json in users:
        filters = json.loads(filters_json)
        city = filters.get('city')
        districts = filters.get('districts', [])
        metros = filters.get('metros', [])
        if not city:
            continue

        ads = fetch_cian(city, districts, metros)
        if not ads:
            continue

        cursor.execute('SELECT last_ad_id FROM users WHERE user_id = ?', (user_id,))
        row = cursor.fetchone()
        last_ad_id = row[0] if row else None

        new_ads = [ad for ad in ads if ad['id'] != last_ad_id]

        for ad in new_ads[:3]:
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
            try:
                from telegram import Bot
                bot = Bot(TOKEN)
                bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown', disable_web_page_preview=True)
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

    # Основные обработчики
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('status', my_status))
    application.add_handler(CallbackQueryHandler(my_status, pattern='^my_status$'))

    application.add_handler(CallbackQueryHandler(choose_plan, pattern='^choose_plan$'))
    application.add_handler(CallbackQueryHandler(plan_chosen, pattern='^plan_'))
    # Оплата TON вызывается внутри plan_chosen

    application.add_handler(CallbackQueryHandler(back_to_start, pattern='^back_to_start$'))

    # Фильтры
    application.add_handler(CallbackQueryHandler(start_filter, pattern='^start_filter$'))
    application.add_handler(CallbackQueryHandler(filter_city_chosen, pattern='^filter_city_'))
    application.add_handler(CallbackQueryHandler(toggle_district, pattern='^toggle_district_'))
    application.add_handler(CallbackQueryHandler(filter_districts_done, pattern='^filter_districts_done$'))
    application.add_handler(CallbackQueryHandler(toggle_metro, pattern='^toggle_metro_'))
    application.add_handler(CallbackQueryHandler(filter_metros_done, pattern='^filter_metros_done$'))

    # Платёжные подтверждения
    application.add_handler(MessageHandler(filters.PHOTO, handle_payment_proof))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_payment_proof))

    # Админские команды
    application.add_handler(CommandHandler('activate', activate_command))
    application.add_handler(CommandHandler('grant', grant_subscription))
    application.add_handler(CommandHandler('stats', stats_command))
    application.add_handler(CommandHandler('users', users_command))
    application.add_handler(CommandHandler('find', find_user_command))
    application.add_handler(CommandHandler('broadcast', broadcast_command))
    application.add_handler(CallbackQueryHandler(broadcast_confirm, pattern='^broadcast_confirm$'))
    application.add_handler(CallbackQueryHandler(broadcast_cancel, pattern='^broadcast_cancel$'))

    logger.info("🚀 Бот успешно запущен и готов к работе (оплата в TON)")
    application.run_polling()

if __name__ == '__main__':
    main()
