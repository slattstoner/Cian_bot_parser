import os
import logging
import json
import requests
import schedule
import time
import random
import re
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import quote
import psycopg2
import psycopg2.extras
from bs4 import BeautifulSoup
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
TON_WALLET = os.environ.get('TON_WALLET', '')
DADATA_API_KEY = os.environ.get('DADATA_API_KEY', '')
DATABASE_URL = os.environ.get('DATABASE_URL')
PORT = int(os.environ.get('PORT', 10000))

if not TOKEN or not ADMIN_ID:
    raise ValueError("Задайте переменные окружения TOKEN и ADMIN_ID")
if not TON_WALLET:
    raise ValueError("Задайте переменную TON_WALLET")
if not DATABASE_URL:
    raise ValueError("Задайте переменную DATABASE_URL для подключения к PostgreSQL")

# Цены подписок в TON
PRICES_TON = {
    '1month': 1.5,
    '3months': 4.0,
    '6months': 7.5,
    '12months': 14.0
}

# Длительность подписок в днях
PLAN_DAYS = {
    '1month': 30,
    '3months': 90,
    '6months': 180,
    '12months': 360
}

# ========== ТОЛЬКО МОСКВА ==========
CITIES = ['Москва']

# Округа Москвы
DISTRICTS = ['ЦАО', 'САО', 'СВАО', 'ВАО', 'ЮВАО', 'ЮАО', 'ЮЗАО', 'ЗАО', 'СЗАО']

# Ветки метро Москвы с короткими кодами (чтобы не превышать лимит callback_data)
METRO_LINES = {
    'line_ap': {
        'name': '🚇 Арбатско-Покровская',
        'stations': [
            "Арбатская", "Площадь Революции", "Курская", "Бауманская", "Электрозаводская",
            "Семёновская", "Партизанская", "Измайловская", "Первомайская", "Щёлковская"
        ]
    },
    'line_zam': {
        'name': '🚇 Замоскворецкая',
        'stations': [
            "Ховрино", "Беломорская", "Речной вокзал", "Водный стадион", "Войковская",
            "Сокол", "Аэропорт", "Динамо", "Белорусская", "Маяковская", "Тверская",
            "Театральная", "Новокузнецкая", "Павелецкая", "Автозаводская", "Технопарк",
            "Коломенская", "Каширская", "Кантемировская", "Царицыно", "Орехово",
            "Домодедовская", "Красногвардейская", "Алма-Атинская"
        ]
    },
    'line_sok': {
        'name': '🚇 Сокольническая',
        'stations': [
            "Бульвар Рокоссовского", "Черкизовская", "Преображенская площадь", "Сокольники",
            "Красносельская", "Комсомольская", "Красные ворота", "Чистые пруды", "Лубянка",
            "Охотный ряд", "Библиотека им. Ленина", "Кропоткинская", "Парк культуры",
            "Фрунзенская", "Спортивная", "Воробьёвы горы", "Университет",
            "Проспект Вернадского", "Юго-Западная", "Тропарёво", "Румянцево", "Саларьево",
            "Филатов Луг", "Прокшино", "Ольховая", "Новомосковская", "Потапово"
        ]
    },
    'line_tag': {
        'name': '🚇 Таганско-Краснопресненская',
        'stations': [
            "Планерная", "Сходненская", "Тушинская", "Щукинская", "Октябрьское поле",
            "Полежаевская", "Беговая", "Улица 1905 года", "Баррикадная", "Пушкинская",
            "Кузнецкий мост", "Китай-город", "Таганская", "Пролетарская", "Волгоградский проспект",
            "Текстильщики", "Кузьминки", "Рязанский проспект", "Выхино", "Лермонтовский проспект",
            "Жулебино", "Котельники"
        ]
    },
    'line_kal': {
        'name': '🚇 Калининская',
        'stations': [
            "Новокосино", "Новогиреево", "Перово", "Шоссе Энтузиастов", "Авиамоторная",
            "Площадь Ильича", "Марксистская", "Третьяковская"
        ]
    },
    'line_sol': {
        'name': '🚇 Солнцевская',
        'stations': [
            "Деловой центр", "Парк Победы", "Минская", "Ломоносовский проспект",
            "Раменки", "Мичуринский проспект", "Озёрная", "Говорово", "Солнцево",
            "Боровское шоссе", "Новопеределкино", "Рассказовка", "Пыхтино", "Аэропорт Внуково"
        ]
    }
}

# Соответствие полных названий округов сокращениям (для определения из адреса)
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

# ========== НАСТРОЙКА ЛОГИРОВАНИЯ ==========
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========== ПОДКЛЮЧЕНИЕ К POSTGRESQL ==========
def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    conn.autocommit = True
    return conn

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            filters TEXT,
            subscribed_until BIGINT,
            last_ad_id TEXT,
            plan TEXT
        )
    ''')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS payments (
            id SERIAL PRIMARY KEY,
            user_id BIGINT,
            amount_ton REAL,
            plan TEXT,
            txid TEXT,
            status TEXT DEFAULT 'pending'
        )
    ''')
    try:
        cur.execute('ALTER TABLE users ADD COLUMN plan TEXT')
    except psycopg2.errors.DuplicateColumn:
        pass
    cur.close()
    conn.close()
    logger.info("База данных инициализирована")

init_db()

# ========== ФУНКЦИИ РАБОТЫ С БД ==========
def get_user(user_id):
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute('SELECT filters, subscribed_until, last_ad_id, plan FROM users WHERE user_id = %s', (user_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    if row:
        return (row['filters'], row['subscribed_until'], row['last_ad_id'], row['plan'])
    return None

def set_user_filters(user_id, filters_dict):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('''
        INSERT INTO users (user_id, filters) VALUES (%s, %s)
        ON CONFLICT (user_id) DO UPDATE SET filters = EXCLUDED.filters
    ''', (user_id, json.dumps(filters_dict)))
    cur.close()
    conn.close()

def activate_subscription(user_id, days, plan=None):
    import time
    until = int(time.time()) + days * 86400
    conn = get_db_connection()
    cur = conn.cursor()
    if plan:
        cur.execute('UPDATE users SET subscribed_until = %s, plan = %s WHERE user_id = %s',
                    (until, plan, user_id))
    else:
        cur.execute('UPDATE users SET subscribed_until = %s WHERE user_id = %s', (until, user_id))
    cur.close()
    conn.close()

def is_subscribed(user_id):
    user = get_user(user_id)
    if user and user[1]:
        import time
        return user[1] > time.time()
    return False

def update_last_ad(user_id, ad_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('UPDATE users SET last_ad_id = %s WHERE user_id = %s', (ad_id, user_id))
    cur.close()
    conn.close()

def get_total_income():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(SUM(amount_ton), 0) FROM payments WHERE status = 'confirmed'")
    total = cur.fetchone()[0]
    cur.close()
    conn.close()
    return total

# ========== ГЕОКОДИНГ (определение округа по адресу через DaData) ==========
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

# ========== HTTP-СЕРВЕР ДЛЯ RENDER ==========
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"OK")
    def log_message(self, format, *args):
        return

def run_http_server():
    server = HTTPServer(('0.0.0.0', PORT), HealthCheckHandler)
    logger.info(f"HTTP-сервер запущен на порту {PORT} для проверок Render")
    server.serve_forever()

# ========== ОБРАБОТЧИКИ КОМАНД ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome_text = (
        "👋 Добро пожаловать в **Realty Parser Bot**!\n\n"
        "🔍 Я отслеживаю **новые объявления о квартирах от собственников** на ЦИАН (Москва) и присылаю их вам сразу после публикации.\n\n"
        "📦 В каждом сообщении:\n"
        "• Ссылка на объявление\n"
        "• Цена, адрес, метро, этаж, площадь\n"
        "• Отметка: собственник или агент\n"
        "• Первые 3 фото\n\n"
        "⚙️ Чтобы начать, оформите подписку и настройте фильтры.\n\n"
        "💎 Оплата принимается в **TON**."
    )
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='choose_plan')],
        [InlineKeyboardButton("ℹ️ Мой статус", callback_data='my_status')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='start_filter')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text(welcome_text, parse_mode='Markdown', reply_markup=reply_markup)

async def my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
            city = filters.get('city', 'Москва')
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
    # Город только Москва, так что сразу переходим к округам
    city = 'Москва'
    context.user_data['filter_city'] = city
    context.user_data['selected_districts'] = []
    context.user_data['selected_metros'] = []

    keyboard = []
    for d in DISTRICTS:
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
    keyboard = []
    for d in DISTRICTS:
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
    await show_metro_lines(update, context)

async def show_metro_lines(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показывает список веток метро для Москвы."""
    query = update.callback_query
    context.user_data['metro_selection_mode'] = 'lines'
    keyboard = []
    # Используем короткие коды линий как callback_data, а показываем красивое имя
    for line_code, line_data in METRO_LINES.items():
        keyboard.append([InlineKeyboardButton(line_data['name'], callback_data=f'metro_line_{line_code}')])
    keyboard.append([InlineKeyboardButton("✅ Готово (метро)", callback_data='filter_metros_done')])
    keyboard.append([InlineKeyboardButton("⏩ Пропустить метро", callback_data='filter_metros_done')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        "🚇 Выберите ветку метро, затем отмечайте нужные станции.\n"
        "После выбора всех станций нажмите **✅ Готово (метро)**.",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def metro_line_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Пользователь выбрал ветку метро. Показываем станции этой ветки."""
    query = update.callback_query
    await query.answer()
    line_code = query.data.split('_', 2)[2]  # metro_line_line_ap -> line_ap
    context.user_data['current_line'] = line_code
    line_data = METRO_LINES[line_code]
    stations = line_data['stations']
    line_name = line_data['name']

    selected_metros = context.user_data.get('selected_metros', [])

    keyboard = []
    for station in stations:
        if station in selected_metros:
            keyboard.append([InlineKeyboardButton(f"✅ {station}", callback_data=f'toggle_metro_station_{station}')])
        else:
            keyboard.append([InlineKeyboardButton(f"⬜️ {station}", callback_data=f'toggle_metro_station_{station}')])
    keyboard.append([InlineKeyboardButton("« Назад к веткам", callback_data='metro_back_to_lines')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"🚇 **{line_name}**\nВыберите станции:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def toggle_metro_station(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отметить/снять отметку станции на текущей ветке."""
    query = update.callback_query
    await query.answer()
    station = query.data.split('_', 3)[3]  # toggle_metro_station_Арбатская
    selected = context.user_data.get('selected_metros', [])
    if station in selected:
        selected.remove(station)
    else:
        selected.append(station)
    context.user_data['selected_metros'] = selected

    line_code = context.user_data['current_line']
    line_data = METRO_LINES[line_code]
    stations = line_data['stations']
    line_name = line_data['name']
    
    keyboard = []
    for s in stations:
        if s in selected:
            keyboard.append([InlineKeyboardButton(f"✅ {s}", callback_data=f'toggle_metro_station_{s}')])
        else:
            keyboard.append([InlineKeyboardButton(f"⬜️ {s}", callback_data=f'toggle_metro_station_{s}')])
    keyboard.append([InlineKeyboardButton("« Назад к веткам", callback_data='metro_back_to_lines')])
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text(
        f"🚇 **{line_name}**\nВыберите станции:",
        parse_mode='Markdown',
        reply_markup=reply_markup
    )

async def metro_back_to_lines(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Возврат к списку веток."""
    query = update.callback_query
    await query.answer()
    await show_metro_lines(update, context)

async def filter_metros_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    await save_filters_and_finish(update, context)

async def save_filters_and_finish(update, context):
    query = update.callback_query
    user_id = query.from_user.id
    city = context.user_data.get('filter_city', 'Москва')
    districts = context.user_data.get('selected_districts', [])
    metros = context.user_data.get('selected_metros', [])

    filters_dict = {
        'city': city,
        'districts': districts,
        'metros': metros
    }
    set_user_filters(user_id, filters_dict)

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

# ---------- ВЫБОР ПЛАНА ПОДПИСКИ (TON) ----------
async def choose_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    keyboard = [
        [InlineKeyboardButton(f"1 месяц – {PRICES_TON['1month']} TON", callback_data='plan_1month')],
        [InlineKeyboardButton(f"3 месяца – {PRICES_TON['3months']} TON", callback_data='plan_3months')],
        [InlineKeyboardButton(f"6 месяцев – {PRICES_TON['6months']} TON", callback_data='plan_6months')],
        [InlineKeyboardButton(f"12 месяцев – {PRICES_TON['12months']} TON", callback_data='plan_12months')],
        [InlineKeyboardButton("« Назад", callback_data='back_to_start')]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await query.edit_message_text("📅 Выберите срок подписки:", reply_markup=reply_markup)

async def plan_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    plan = query.data.split('_')[1]
    context.user_data['plan'] = plan
    await pay_ton(update, context)

async def pay_ton(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = query.from_user.id
    plan = context.user_data.get('plan', '1month')
    amount_ton = PRICES_TON[plan]

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('INSERT INTO payments (user_id, amount_ton, plan) VALUES (%s, %s, %s) RETURNING id',
                (user_id, amount_ton, plan))
    payment_id = cur.fetchone()[0]
    cur.close()
    conn.close()

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
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('UPDATE payments SET txid = %s WHERE user_id = %s AND status = %s',
                    (txid, user_id, 'pending'))
        cur.close()
        conn.close()
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
        "🔍 Я отслеживаю **новые объявления о квартирах от собственников** на ЦИАН (Москва) и присылаю их вам сразу после публикации.\n\n"
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
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute('SELECT plan FROM payments WHERE user_id = %s AND status = %s ORDER BY id DESC LIMIT 1',
                    (user_id, 'pending'))
        row = cur.fetchone()
        if row:
            plan = row[0]
            days = PLAN_DAYS[plan]
            activate_subscription(user_id, days, plan)
            cur.execute('UPDATE payments SET status = %s WHERE user_id = %s AND status = %s',
                        ('confirmed', user_id, 'pending'))
            conn.commit()
            cur.close()
            conn.close()
            await update.message.reply_text(f"✅ Подписка для {user_id} активирована на {days} дней.")
            await context.bot.send_message(
                chat_id=user_id,
                text="✅ Ваша подписка активирована! Теперь настройте фильтры в главном меню."
            )
        else:
            cur.close()
            conn.close()
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
        plan = context.args[2] if len(context.args) > 2 else None
        if plan and plan not in PRICES_TON:
            await update.message.reply_text("❌ Неверный план. Допустимые: 1month, 3months, 6months, 12months")
            return
        activate_subscription(user_id, days, plan)
        await update.message.reply_text(f"✅ Подписка для пользователя {user_id} активирована на {days} дней.")
        msg = f"✅ Администратор выдал вам подписку на {days} дней! Настройте фильтры в главном меню."
        await context.bot.send_message(chat_id=user_id, text=msg)
    except (IndexError, ValueError):
        await update.message.reply_text("❌ Использование: /grant user_id days [plan]\nПример: /grant 123456789 30 1month")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def stats_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    import time
    now = int(time.time())
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM users WHERE subscribed_until > %s", (now,))
    active_subs = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM payments WHERE status = 'pending'")
    pending_payments = cur.fetchone()[0]
    cur.execute("SELECT COALESCE(SUM(amount_ton), 0) FROM payments WHERE status = 'confirmed'")
    total_income = cur.fetchone()[0]

    cur.execute("SELECT plan FROM users WHERE subscribed_until > %s AND plan IS NOT NULL", (now,))
    active_plans = cur.fetchall()
    monthly_income = 0.0
    for (plan,) in active_plans:
        if plan in PRICES_TON and plan in PLAN_DAYS:
            price_per_month = PRICES_TON[plan] / PLAN_DAYS[plan] * 30
            monthly_income += price_per_month
    cur.close()
    conn.close()

    text = (
        f"📊 **Статистика бота**\n"
        f"👥 Всего пользователей: {total_users}\n"
        f"✅ Активных подписок: {active_subs}\n"
        f"💰 Ежемесячный доход от активных подписок: **{monthly_income:.2f} TON**\n"
        f"💵 Общий доход за всё время: **{total_income:.2f} TON**\n"
        f"⏳ Ожидающих платежей: {pending_payments}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')

async def users_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id, subscribed_until, plan FROM users ORDER BY user_id LIMIT 20")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    if not rows:
        await update.message.reply_text("Нет пользователей.")
        return
    text = "**Список пользователей (первые 20):**\n"
    import time
    now = int(time.time())
    for user_id, until, plan in rows:
        if until and until > now:
            remaining = (until - now) // 86400
            status = f"✅ активна (осталось {remaining} дн.)"
        else:
            status = "❌ не активна"
        plan_str = f", план: {plan}" if plan else ""
        text += f"• `{user_id}` — {status}{plan_str}\n"
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
        filters_json, subscribed_until, last_ad_id, plan = user
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
            f"План: {plan if plan else 'не указан'}\n"
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
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM users")
    users = cur.fetchall()
    cur.close()
    conn.close()
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

async def test_parse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда для ручного запуска парсинга и получения отчёта (только для админа)."""
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("🔄 Запускаю парсинг...")
    try:
        check_new_ads()
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")
        logger.error(f"Ошибка в test_parse: {e}")
    else:
        await update.message.reply_text("✅ Парсинг завершён. Проверьте логи.")

    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT user_id, filters FROM users WHERE subscribed_until > extract(epoch from now())")
    users = cur.fetchall()
    cur.close()
    conn.close()
    if users:
        await update.message.reply_text(f"Активных подписчиков: {len(users)}")
        for user_id, filters_json in users:
            filters = json.loads(filters_json)
            city = filters.get('city', 'Москва')
            await update.message.reply_text(f"👤 {user_id}: {city}, округов {len(filters.get('districts', []))}, станций {len(filters.get('metros', []))}")
    else:
        await update.message.reply_text("Нет активных подписчиков.")

# ========== САМОПИСНЫЙ ПАРСЕР ==========
def fetch_cian(districts, metros):
    """
    Парсер ЦИАН на requests и BeautifulSoup (только Москва).
    Возвращает список словарей с данными объявлений.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'none',
        'Cache-Control': 'max-age=0'
    }
    
    base_url = "https://www.cian.ru/cat.php"
    
    # Параметры запроса (Москва = регион 1)
    params = {
        'deal_type': 'sale',
        'engine_version': '2',
        'offer_type': 'flat',
        'region': '1',
        'only_flat': '1',
        'owner': '1',
        'sort': 'creation_date_desc',
        'p': '1'
    }
    
    # Добавляем округа в параметры (коды округов ЦИАН)
    district_codes = {
        'ЦАО': 8, 'САО': 9, 'СВАО': 10, 'ВАО': 11, 'ЮВАО': 12,
        'ЮАО': 13, 'ЮЗАО': 14, 'ЗАО': 15, 'СЗАО': 16
    }
    for district in districts:
        if district in district_codes:
            params[f'okrug[{district_codes[district]}]'] = '1'
    
    logger.info(f"Парсинг URL: {base_url} с параметрами {params}")
    
    try:
        time.sleep(random.uniform(1, 3))  # задержка
        response = requests.get(base_url, params=params, headers=headers, timeout=15)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Поиск карточек объявлений
        cards = soup.find_all('article', {'data-name': 'CardComponent'})
        if not cards:
            cards = soup.find_all('div', class_=re.compile('_93444fe79c--card--'))
        
        logger.info(f"Найдено карточек: {len(cards)}")
        
        results = []
        for card in cards[:10]:  # не более 10 за раз
            try:
                # Ссылка
                link_tag = card.find('a', href=True)
                if not link_tag:
                    continue
                link = link_tag['href']
                if not link.startswith('http'):
                    link = 'https://www.cian.ru' + link
                
                # ID из ссылки
                ad_id_match = re.search(r'/(\d+)/?$', link)
                ad_id = ad_id_match.group(1) if ad_id_match else str(hash(link))
                
                # Цена
                price_tag = card.find('span', {'data-mark': 'MainPrice'})
                if not price_tag:
                    price_tag = card.find('span', class_=re.compile('price'))
                price = price_tag.text.strip() if price_tag else 'Цена не указана'
                
                # Адрес
                address_tag = card.find('address')
                if not address_tag:
                    address_tag = card.find('span', class_=re.compile('address'))
                address = address_tag.text.strip() if address_tag else 'Москва'
                
                # Метро
                metro_tag = card.find('span', class_=re.compile('underground'))
                if not metro_tag:
                    metro_tag = card.find('a', href=re.compile('metro'))
                metro = metro_tag.text.strip() if metro_tag else 'Не указано'
                
                # Характеристики
                title_tag = card.find('h3')
                title = title_tag.text.strip() if title_tag else 'Квартира'
                
                chars = card.find_all('span', class_=re.compile('characteristic'))
                chars_text = ' '.join([c.text for c in chars])
                
                floor = '?/?'
                floor_match = re.search(r'(\d+)\s*этаж\s*из\s*(\d+)', chars_text)
                if floor_match:
                    floor = f"{floor_match.group(1)}/{floor_match.group(2)}"
                
                area = '? м²'
                area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', chars_text)
                if area_match:
                    area = f"{area_match.group(1)} м²"
                
                rooms = '?'
                rooms_match = re.search(r'(\d+)[-\s]комнат', title.lower())
                if rooms_match:
                    rooms = rooms_match.group(1)
                
                # Собственник?
                owner_tag = card.find('span', text=re.compile('собственник', re.I))
                is_owner = bool(owner_tag)
                
                # Фото
                photos = []
                img_tags = card.find_all('img', src=True)
                for img in img_tags[:3]:
                    img_url = img['src']
                    if img_url.startswith('//'):
                        img_url = 'https:' + img_url
                    if 'avatar' not in img_url and not img_url.endswith('.svg'):
                        photos.append(img_url)
                
                # Округ (определяем по адресу)
                district_detected = None
                if DADATA_API_KEY:
                    district_detected = get_district_by_address(address)
                
                ad = {
                    'id': ad_id,
                    'title': title,
                    'link': link,
                    'price': price,
                    'address': address,
                    'metro': metro,
                    'floor': floor,
                    'area': area,
                    'rooms': rooms,
                    'owner': is_owner,
                    'photos': photos,
                    'district_detected': district_detected
                }
                results.append(ad)
                
            except Exception as e:
                logger.error(f"Ошибка парсинга карточки: {e}")
                continue
        
        logger.info(f"Успешно распарсено {len(results)} объявлений")
        return results
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка HTTP: {e}")
        return []
    except Exception as e:
        logger.error(f"Неожиданная ошибка: {e}")
        return []

def check_new_ads():
    import time
    now = int(time.time())
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT user_id, filters FROM users WHERE subscribed_until > %s', (now,))
    users = cur.fetchall()
    cur.close()
    conn.close()
    
    for user_id, filters_json in users:
        filters = json.loads(filters_json)
        districts = filters.get('districts', [])
        metros = filters.get('metros', [])
        
        ads = fetch_cian(districts, metros)
        if not ads:
            continue
        
        conn2 = get_db_connection()
        cur2 = conn2.cursor()
        cur2.execute('SELECT last_ad_id FROM users WHERE user_id = %s', (user_id,))
        row = cur2.fetchone()
        last_ad_id = row[0] if row else None
        cur2.close()
        conn2.close()
        
        new_ads = [ad for ad in ads if ad['id'] != last_ad_id]
        
        for ad in new_ads[:3]:
            # Проверяем соответствие фильтрам
            district_ok = True
            if districts and ad['district_detected']:
                district_ok = ad['district_detected'] in districts
            
            metro_ok = True
            if metros and ad['metro'] != 'Не указано':
                metro_ok = ad['metro'] in metros
            
            if (not districts and not metros) or district_ok or metro_ok:
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
                    if ad['photos']:
                        for photo in ad['photos'][:3]:
                            bot.send_photo(chat_id=user_id, photo=photo)
                    update_last_ad(user_id, ad['id'])
                except Exception as e:
                    logger.error(f"Ошибка отправки {user_id}: {e}")

def run_schedule():
    schedule.every(10).minutes.do(check_new_ads)
    while True:
        schedule.run_pending()
        time.sleep(1)

# ========== ЗАПУСК ==========
def main():
    http_thread = Thread(target=run_http_server, daemon=True)
    http_thread.start()

    Thread(target=run_schedule, daemon=True).start()

    application = Application.builder().token(TOKEN).build()

    # Основные обработчики
    application.add_handler(CommandHandler('start', start))
    application.add_handler(CommandHandler('status', my_status))
    application.add_handler(CallbackQueryHandler(my_status, pattern='^my_status$'))

    application.add_handler(CallbackQueryHandler(choose_plan, pattern='^choose_plan$'))
    application.add_handler(CallbackQueryHandler(plan_chosen, pattern='^plan_'))

    application.add_handler(CallbackQueryHandler(back_to_start, pattern='^back_to_start$'))

    # Фильтры
    application.add_handler(CallbackQueryHandler(start_filter, pattern='^start_filter$'))
    application.add_handler(CallbackQueryHandler(toggle_district, pattern='^toggle_district_'))
    application.add_handler(CallbackQueryHandler(filter_districts_done, pattern='^filter_districts_done$'))

    # Метро по веткам
    application.add_handler(CallbackQueryHandler(metro_line_chosen, pattern='^metro_line_'))
    application.add_handler(CallbackQueryHandler(toggle_metro_station, pattern='^toggle_metro_station_'))
    application.add_handler(CallbackQueryHandler(metro_back_to_lines, pattern='^metro_back_to_lines$'))
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
    application.add_handler(CommandHandler('test_parse', test_parse))
    application.add_handler(CallbackQueryHandler(broadcast_confirm, pattern='^broadcast_confirm$'))
    application.add_handler(CallbackQueryHandler(broadcast_cancel, pattern='^broadcast_cancel$'))

    logger.info("🚀 Бот успешно запущен (только Москва, собственный парсер)")
    application.run_polling()

if __name__ == '__main__':
    main()
