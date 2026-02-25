import os
import logging
import json
import asyncio
import time
import random
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode

import aiohttp
import asyncpg
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

# ========== НАСТРОЙКИ ==========
TOKEN = os.environ.get('TOKEN')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 0))
TON_WALLET = os.environ.get('TON_WALLET', '')
DADATA_API_KEY = os.environ.get('DADATA_API_KEY', '')
DATABASE_URL = os.environ.get('DATABASE_URL')

if not TOKEN or not ADMIN_ID:
    raise ValueError("Задайте TOKEN и ADMIN_ID")
if not TON_WALLET:
    raise ValueError("Задайте TON_WALLET")
if not DATABASE_URL:
    raise ValueError("Задайте DATABASE_URL")

# Цены подписок в TON
PRICES_TON = {
    '1m': 1.5,
    '3m': 4.0,
    '6m': 7.5,
    '12m': 14.0
}
PLAN_DAYS = {'1m': 30, '3m': 90, '6m': 180, '12m': 360}

# ========== ДАННЫЕ ПО МОСКВЕ ==========
DISTRICTS = ['ЦАО', 'САО', 'СВАО', 'ВАО', 'ЮВАО', 'ЮАО', 'ЮЗАО', 'ЗАО', 'СЗАО']

METRO_LINES = {
    'ap': {'name': '🚇 Арбатско-Покровская',
           'stations': ["Арбатская", "Площадь Революции", "Курская", "Бауманская", "Электрозаводская",
                        "Семёновская", "Партизанская", "Измайловская", "Первомайская", "Щёлковская"]},
    'zam': {'name': '🚇 Замоскворецкая',
            'stations': ["Ховрино", "Беломорская", "Речной вокзал", "Водный стадион", "Войковская",
                         "Сокол", "Аэропорт", "Динамо", "Белорусская", "Маяковская", "Тверская",
                         "Театральная", "Новокузнецкая", "Павелецкая", "Автозаводская", "Технопарк",
                         "Коломенская", "Каширская", "Кантемировская", "Царицыно", "Орехово",
                         "Домодедовская", "Красногвардейская", "Алма-Атинская"]},
    'sok': {'name': '🚇 Сокольническая',
            'stations': ["Бульвар Рокоссовского", "Черкизовская", "Преображенская площадь", "Сокольники",
                         "Красносельская", "Комсомольская", "Красные ворота", "Чистые пруды", "Лубянка",
                         "Охотный ряд", "Библиотека им. Ленина", "Кропоткинская", "Парк культуры",
                         "Фрунзенская", "Спортивная", "Воробьёвы горы", "Университет",
                         "Проспект Вернадского", "Юго-Западная", "Тропарёво", "Румянцево", "Саларьево",
                         "Филатов Луг", "Прокшино", "Ольховая", "Новомосковская", "Потапово"]},
    'tag': {'name': '🚇 Таганско-Краснопресненская',
            'stations': ["Планерная", "Сходненская", "Тушинская", "Щукинская", "Октябрьское поле",
                         "Полежаевская", "Беговая", "Улица 1905 года", "Баррикадная", "Пушкинская",
                         "Кузнецкий мост", "Китай-город", "Таганская", "Пролетарская", "Волгоградский проспект",
                         "Текстильщики", "Кузьминки", "Рязанский проспект", "Выхино", "Лермонтовский проспект",
                         "Жулебино", "Котельники"]},
    'kal': {'name': '🚇 Калининская',
            'stations': ["Новокосино", "Новогиреево", "Перово", "Шоссе Энтузиастов", "Авиамоторная",
                         "Площадь Ильича", "Марксистская", "Третьяковская"]},
    'sol': {'name': '🚇 Солнцевская',
            'stations': ["Деловой центр", "Парк Победы", "Минская", "Ломоносовский проспект",
                         "Раменки", "Мичуринский проспект", "Озёрная", "Говорово", "Солнцево",
                         "Боровское шоссе", "Новопеределкино", "Рассказовка", "Пыхтино", "Аэропорт Внуково"]}
}

DISTRICT_CODES = {d: i for i, d in enumerate([8,9,10,11,12,13,14,15,16], start=8)}  # коды ЦИАН

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

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========== ПУЛ БД ==========
class Database:
    _pool = None

    @classmethod
    async def init(cls):
        cls._pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    filters TEXT,
                    subscribed_until BIGINT,
                    last_ad_id TEXT,
                    plan TEXT
                )
            ''')
            await conn.execute('''
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
                await conn.execute('ALTER TABLE users ADD COLUMN plan TEXT')
            except asyncpg.exceptions.DuplicateColumnError:
                pass
        logger.info("База данных инициализирована")

    @classmethod
    async def get_user(cls, user_id):
        async with cls._pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT filters, subscribed_until, last_ad_id, plan FROM users WHERE user_id = $1',
                user_id
            )
            if row:
                return (row['filters'], row['subscribed_until'], row['last_ad_id'], row['plan'])
            return None

    @classmethod
    async def set_user_filters(cls, user_id, filters_dict):
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, filters) VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE SET filters = EXCLUDED.filters
            ''', user_id, json.dumps(filters_dict))

    @classmethod
    async def activate_subscription(cls, user_id, days, plan=None):
        until = int(time.time()) + days * 86400
        async with cls._pool.acquire() as conn:
            if plan:
                await conn.execute(
                    'UPDATE users SET subscribed_until = $1, plan = $2 WHERE user_id = $3',
                    until, plan, user_id
                )
            else:
                await conn.execute(
                    'UPDATE users SET subscribed_until = $1 WHERE user_id = $2',
                    until, user_id
                )

    @classmethod
    async def update_last_ad(cls, user_id, ad_id):
        async with cls._pool.acquire() as conn:
            await conn.execute(
                'UPDATE users SET last_ad_id = $1 WHERE user_id = $2',
                ad_id, user_id
            )

    @classmethod
    async def add_payment(cls, user_id, amount_ton, plan):
        async with cls._pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO payments (user_id, amount_ton, plan) VALUES ($1, $2, $3) RETURNING id',
                user_id, amount_ton, plan
            )

    @classmethod
    async def update_payment_txid(cls, user_id, txid):
        async with cls._pool.acquire() as conn:
            await conn.execute(
                'UPDATE payments SET txid = $1 WHERE user_id = $2 AND status = $3',
                txid, user_id, 'pending'
            )

    @classmethod
    async def confirm_payment(cls, user_id, plan):
        async with cls._pool.acquire() as conn:
            await conn.execute(
                'UPDATE payments SET status = $1 WHERE user_id = $2 AND status = $3',
                'confirmed', user_id, 'pending'
            )

    @classmethod
    async def get_pending_plan(cls, user_id):
        async with cls._pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT plan FROM payments WHERE user_id = $1 AND status = $2 ORDER BY id DESC LIMIT 1',
                user_id, 'pending'
            )
            return row['plan'] if row else None

    @classmethod
    async def get_stats(cls):
        now = int(time.time())
        async with cls._pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM users')
            active = await conn.fetchval('SELECT COUNT(*) FROM users WHERE subscribed_until > $1', now)
            pending = await conn.fetchval('SELECT COUNT(*) FROM payments WHERE status = $1', 'pending')
            total_income = await conn.fetchval('SELECT COALESCE(SUM(amount_ton), 0) FROM payments WHERE status = $1', 'confirmed')
            active_plans = await conn.fetch('SELECT plan FROM users WHERE subscribed_until > $1 AND plan IS NOT NULL', now)
            monthly = 0.0
            for (plan,) in active_plans:
                if plan in PRICES_TON and plan in PLAN_DAYS:
                    monthly += PRICES_TON[plan] / PLAN_DAYS[plan] * 30
            return total, active, pending, total_income, monthly

    @classmethod
    async def get_all_users(cls, limit=20):
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT user_id, subscribed_until, plan FROM users ORDER BY user_id LIMIT $1', limit)

    @classmethod
    async def get_active_subscribers(cls):
        now = int(time.time())
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT user_id, filters FROM users WHERE subscribed_until > $1', now)

# ========== КЭШ ПАРСИНГА ==========
parse_cache = {}  # key: tuple(districts_tuple, metros_tuple) -> (data, expiry)

def cache_key(districts, metros):
    return (tuple(sorted(districts)), tuple(sorted(metros)))

async def fetch_cian(districts, metros):
    """Асинхронный парсинг ЦИАН с кэшированием."""
    key = cache_key(districts, metros)
    now = time.time()
    if key in parse_cache and parse_cache[key][1] > now:
        logger.info("Использую кэш парсинга")
        return parse_cache[key][0]

    headers = {
        'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1',
        'Accept-Language': 'ru-RU,ru;q=0.9'
    }
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
    for d in districts:
        code = {'ЦАО':8, 'САО':9, 'СВАО':10, 'ВАО':11, 'ЮВАО':12, 'ЮАО':13, 'ЮЗАО':14, 'ЗАО':15, 'СЗАО':16}.get(d)
        if code:
            params[f'okrug[{code}]'] = '1'

    url = "https://www.cian.ru/cat.php?" + urlencode(params)
    logger.info(f"Парсинг: {url}")

    try:
        await asyncio.sleep(random.uniform(1, 2))  # вежливая задержка
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=15) as resp:
                text = await resp.text()

        soup = BeautifulSoup(text, 'lxml')
        cards = soup.find_all('article', {'data-name': 'CardComponent'})
        if not cards:
            cards = soup.find_all('div', class_=re.compile('_93444fe79c--card--'))

        results = []
        for card in cards[:10]:
            try:
                link_tag = card.find('a', href=True)
                if not link_tag:
                    continue
                link = link_tag['href']
                if not link.startswith('http'):
                    link = 'https://www.cian.ru' + link
                ad_id = re.search(r'/(\d+)/?$', link)
                ad_id = ad_id.group(1) if ad_id else str(hash(link))

                price_tag = card.find('span', {'data-mark': 'MainPrice'}) or card.find('span', class_=re.compile('price'))
                price = price_tag.text.strip() if price_tag else 'Цена не указана'

                address_tag = card.find('address') or card.find('span', class_=re.compile('address'))
                address = address_tag.text.strip() if address_tag else 'Москва'

                metro_tag = card.find('span', class_=re.compile('underground')) or card.find('a', href=re.compile('metro'))
                metro = metro_tag.text.strip() if metro_tag else 'Не указано'

                title_tag = card.find('h3')
                title = title_tag.text.strip() if title_tag else 'Квартира'

                chars = card.find_all('span', class_=re.compile('characteristic'))
                chars_text = ' '.join(c.text for c in chars)

                floor = '?/?'
                fm = re.search(r'(\d+)\s*этаж\s*из\s*(\d+)', chars_text)
                if fm:
                    floor = f"{fm.group(1)}/{fm.group(2)}"

                area = '? м²'
                am = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', chars_text)
                if am:
                    area = f"{am.group(1)} м²"

                rooms = '?'
                rm = re.search(r'(\d+)[-\s]комнат', title.lower())
                if rm:
                    rooms = rm.group(1)

                is_owner = bool(card.find('span', text=re.compile('собственник', re.I)))

                photos = []
                for img in card.find_all('img', src=True)[:3]:
                    src = img['src']
                    if src.startswith('//'):
                        src = 'https:' + src
                    if 'avatar' not in src and not src.endswith('.svg'):
                        photos.append(src)

                # Определение округа (опционально)
                district_detected = None
                if DADATA_API_KEY:
                    district_detected = await get_district_by_address(address)

                results.append({
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
                })
            except Exception as e:
                logger.error(f"Ошибка парсинга карточки: {e}")

        # Кэшируем на 5 минут
        parse_cache[key] = (results, now + 300)
        return results
    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}")
        return []

async def get_district_by_address(address):
    """Определение округа через DaData."""
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
        async with aiohttp.ClientSession() as sess:
            async with sess.post(url, headers=headers, json=data, timeout=5) as resp:
                res = await resp.json()
        result = res[0]
        if result.get('area_type') == "округ" and result.get('area'):
            return DISTRICT_MAPPING.get(result['area'])
    except Exception as e:
        logger.error(f"Ошибка DaData: {e}")
    return None

# ========== ФОНОВЫЙ ПАРСИНГ ==========
async def background_parser(app: Application):
    """Запускается как фоновая задача в главном цикле."""
    while True:
        try:
            users = await Database.get_active_subscribers()
            for user_id, filters_json in users:
                filters = json.loads(filters_json)
                districts = filters.get('districts', [])
                metros = filters.get('metros', [])
                ads = await fetch_cian(districts, metros)
                if not ads:
                    continue

                user_data = await Database.get_user(user_id)
                last_ad_id = user_data[2] if user_data else None
                new_ads = [a for a in ads if a['id'] != last_ad_id]

                for ad in new_ads[:3]:
                    # Фильтрация по округу и метро
                    district_ok = True
                    if districts and ad.get('district_detected'):
                        district_ok = ad['district_detected'] in districts
                    metro_ok = True
                    if metros and ad['metro'] != 'Не указано':
                        metro_ok = ad['metro'] in metros

                    if (not districts and not metros) or district_ok or metro_ok:
                        owner = "Собственник" if ad['owner'] else "Агент"
                        text = (
                            f"🔵 *Новое объявление*\n{ad['title']}\n"
                            f"💰 Цена: {ad['price']}\n📍 Адрес: {ad['address']}\n"
                            f"🚇 Метро: {ad['metro']}\n🏢 Этаж: {ad['floor']}\n"
                            f"📏 Площадь: {ad['area']}\n🛏 Комнат: {ad['rooms']}\n"
                            f"👤 {owner}\n[Ссылка]({ad['link']})"
                        )
                        try:
                            await app.bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown',
                                                       disable_web_page_preview=True)
                            for photo in ad['photos'][:3]:
                                await app.bot.send_photo(chat_id=user_id, photo=photo)
                            await Database.update_last_ad(user_id, ad['id'])
                        except Exception as e:
                            logger.error(f"Ошибка отправки {user_id}: {e}")
        except Exception as e:
            logger.error(f"Ошибка в фоновом парсинге: {e}")
        await asyncio.sleep(600)  # 10 минут

# ========== ОБРАБОТЧИКИ КОМАНД ==========
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    welcome = (
        "👋 Добро пожаловать в **Realty Parser Bot**!\n\n"
        "🔍 Я отслеживаю **новые объявления о квартирах от собственников** на ЦИАН (Москва) и присылаю их вам сразу после публикации.\n\n"
        "📦 В каждом сообщении: ссылка, цена, адрес, метро, этаж, площадь, комнаты, собственник/агент, фото.\n\n"
        "⚙️ Чтобы начать, оформите подписку и настройте фильтры.\n\n💎 Оплата в **TON**."
    )
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='cp')],
        [InlineKeyboardButton("ℹ️ Мой статус", callback_data='st')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='fl')]
    ]
    await update.message.reply_text(welcome, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def my_status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.callback_query:
        user_id = update.callback_query.from_user.id
        message = update.callback_query.message
        send = message.reply_text
        await update.callback_query.answer()
    else:
        user_id = update.effective_user.id
        send = update.message.reply_text

    user = await Database.get_user(user_id)
    now = int(time.time())
    if user and user[1] and user[1] > now:
        rem = user[1] - now
        days = rem // 86400
        hours = (rem % 86400) // 3600
        status = f"✅ **Подписка активна**\nОсталось: {days} дн. {hours} ч."
    else:
        status = "❌ **Подписка не активна**"

    filters = user[0] if user and user[0] else None
    if filters:
        try:
            f = json.loads(filters)
            city = f.get('city', 'Москва')
            districts = ', '.join(f.get('districts', [])) or 'все'
            metros = ', '.join(f.get('metros', [])) or 'все'
            disp = f"🏙 **Город:** {city}\n🏘 **Округа:** {districts}\n🚇 **Метро:** {metros}"
        except:
            disp = "⚠️ Ошибка в фильтрах"
    else:
        disp = "⚙️ Фильтры не настроены"

    await send(f"{status}\n\n{disp}", parse_mode='Markdown')

# ---------- ВЫБОР ПЛАНА ----------
async def choose_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    keyboard = [
        [InlineKeyboardButton(f"1 мес – {PRICES_TON['1m']} TON", callback_data='p1m')],
        [InlineKeyboardButton(f"3 мес – {PRICES_TON['3m']} TON", callback_data='p3m')],
        [InlineKeyboardButton(f"6 мес – {PRICES_TON['6m']} TON", callback_data='p6m')],
        [InlineKeyboardButton(f"12 мес – {PRICES_TON['12m']} TON", callback_data='p12m')],
        [InlineKeyboardButton("« Назад", callback_data='bk')]
    ]
    await q.edit_message_text("📅 Выберите срок подписки:", reply_markup=InlineKeyboardMarkup(keyboard))

async def plan_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    plan = q.data[1:]  # p1m -> 1m
    context.user_data['plan'] = plan
    await pay_ton(q, context)

async def pay_ton(update, context):
    q = update if isinstance(update, Update) else update
    user_id = q.from_user.id
    plan = context.user_data.get('plan', '1m')
    amount = PRICES_TON[plan]
    payment_id = await Database.add_payment(user_id, amount, plan)
    text = (
        f"**Оплата в TON**\n\nСумма: **{amount} TON**\n"
        f"Кошелёк: `{TON_WALLET}`\n\n"
        "После перевода **отправьте TXID** (или скриншот).\n"
        "Админ проверит и активирует подписку.\n\n"
        f"**ID платежа:** `{payment_id}`"
    )
    await q.edit_message_text(text, parse_mode='Markdown')

# ---------- ФИЛЬТРЫ ----------
async def start_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data['districts'] = []
    context.user_data['metros'] = []
    keyboard = [[InlineKeyboardButton(f"⬜ {d}", callback_data=f'd{d}')] for d in DISTRICTS]
    keyboard.append([InlineKeyboardButton("✅ Готово", callback_data='dfin')])
    await q.edit_message_text("🏘 Выберите округа (можно несколько):", reply_markup=InlineKeyboardMarkup(keyboard))

async def toggle_district(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    district = q.data[1:]  # dЦАО -> ЦАО
    selected = context.user_data.get('districts', [])
    if district in selected:
        selected.remove(district)
    else:
        selected.append(district)
    context.user_data['districts'] = selected
    keyboard = []
    for d in DISTRICTS:
        mark = "✅" if d in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {d}", callback_data=f'd{d}')])
    keyboard.append([InlineKeyboardButton("✅ Готово", callback_data='dfin')])
    await q.edit_message_text("🏘 Выберите округа:", reply_markup=InlineKeyboardMarkup(keyboard))

async def districts_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    # Переход к метро
    keyboard = []
    for code, line in METRO_LINES.items():
        keyboard.append([InlineKeyboardButton(line['name'], callback_data=f'l{code}')])
    keyboard.append([InlineKeyboardButton("✅ Готово", callback_data='mfin')])
    keyboard.append([InlineKeyboardButton("⏩ Пропустить", callback_data='mfin')])
    await q.edit_message_text("🚇 Выберите ветку метро:", reply_markup=InlineKeyboardMarkup(keyboard))

async def metro_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    line_code = q.data[1:]
    context.user_data['cur_line'] = line_code
    line = METRO_LINES[line_code]
    selected = context.user_data.get('metros', [])
    keyboard = []
    for s in line['stations']:
        mark = "✅" if s in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {s}", callback_data=f'm{s}')])
    keyboard.append([InlineKeyboardButton("« Назад к веткам", callback_data='mbk')])
    await q.edit_message_text(f"🚇 **{line['name']}**\nВыберите станции:", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def toggle_metro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    station = q.data[1:]
    selected = context.user_data.get('metros', [])
    if station in selected:
        selected.remove(station)
    else:
        selected.append(station)
    context.user_data['metros'] = selected
    line_code = context.user_data['cur_line']
    line = METRO_LINES[line_code]
    keyboard = []
    for s in line['stations']:
        mark = "✅" if s in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {s}", callback_data=f'm{s}')])
    keyboard.append([InlineKeyboardButton("« Назад к веткам", callback_data='mbk')])
    await q.edit_message_text(f"🚇 **{line['name']}**\nВыберите станции:", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def metro_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    keyboard = []
    for code, line in METRO_LINES.items():
        keyboard.append([InlineKeyboardButton(line['name'], callback_data=f'l{code}')])
    keyboard.append([InlineKeyboardButton("✅ Готово", callback_data='mfin')])
    await q.edit_message_text("🚇 Выберите ветку метро:", reply_markup=InlineKeyboardMarkup(keyboard))

async def metros_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    districts = context.user_data.get('districts', [])
    metros = context.user_data.get('metros', [])
    filters = {'city': 'Москва', 'districts': districts, 'metros': metros}
    await Database.set_user_filters(user_id, filters)

    text = f"✅ **Фильтры сохранены!**\n🏙 Город: Москва\n"
    text += f"🏘 Округа: {', '.join(districts) if districts else 'все'}\n"
    text += f"🚇 Метро: {', '.join(metros) if metros else 'все'}"
    await q.edit_message_text(text, parse_mode='Markdown')
    # Главное меню
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='cp')],
        [InlineKeyboardButton("ℹ️ Мой статус", callback_data='st')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='fl')]
    ]
    await context.bot.send_message(chat_id=user_id, text="Главное меню:", reply_markup=InlineKeyboardMarkup(keyboard))

async def back_to_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='cp')],
        [InlineKeyboardButton("ℹ️ Мой статус", callback_data='st')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='fl')]
    ]
    await q.edit_message_text("👋 Главное меню", reply_markup=InlineKeyboardMarkup(keyboard))

# ---------- ПЛАТЕЖИ ----------
async def handle_payment_proof(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    msg = update.message
    if msg.photo:
        caption = f"Пользователь {user_id} отправил скриншот оплаты TON."
        await context.bot.send_photo(chat_id=ADMIN_ID, photo=msg.photo[-1].file_id, caption=caption)
        await msg.reply_text("✅ Скриншот отправлен администратору. Ожидайте.")
    elif msg.text:
        txid = msg.text.strip()
        await Database.update_payment_txid(user_id, txid)
        await context.bot.send_message(
            chat_id=ADMIN_ID,
            text=f"Пользователь {user_id} отправил TXID: {txid}\nДля активации: /act {user_id}"
        )
        await msg.reply_text("✅ Данные получены. Ожидайте подтверждения.")
    else:
        await msg.reply_text("Отправьте TXID или скриншот.")

# ---------- АДМИНСКИЕ КОМАНДЫ ----------
async def activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        plan = await Database.get_pending_plan(user_id)
        if plan:
            days = PLAN_DAYS[plan]
            await Database.activate_subscription(user_id, days, plan)
            await Database.confirm_payment(user_id, plan)
            await update.message.reply_text(f"✅ Подписка для {user_id} активирована на {days} дней.")
            await context.bot.send_message(
                chat_id=user_id,
                text="✅ Ваша подписка активирована! Настройте фильтры в главном меню."
            )
        else:
            await update.message.reply_text("❌ Нет ожидающих платежей.")
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /act user_id")

async def grant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        days = int(context.args[1])
        plan = context.args[2] if len(context.args) > 2 else None
        if plan and plan not in PRICES_TON:
            await update.message.reply_text("Неверный план. Допустимые: 1m, 3m, 6m, 12m")
            return
        await Database.activate_subscription(user_id, days, plan)
        await update.message.reply_text(f"✅ Подписка для {user_id} на {days} дней.")
        await context.bot.send_message(chat_id=user_id, text=f"✅ Админ выдал подписку на {days} дней! Настройте фильтры.")
    except:
        await update.message.reply_text("Использование: /grant user_id days [plan]")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    total, active, pending, total_income, monthly = await Database.get_stats()
    text = (
        f"📊 **Статистика**\n👥 Всего: {total}\n✅ Активных: {active}\n"
        f"💰 Ежемес. доход: **{monthly:.2f} TON**\n💵 Всего доход: **{total_income:.2f} TON**\n"
        f"⏳ Ожидают: {pending}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')

async def users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    rows = await Database.get_all_users()
    if not rows:
        await update.message.reply_text("Нет пользователей.")
        return
    text = "**Пользователи (первые 20):**\n"
    now = int(time.time())
    for user_id, until, plan in rows:
        status = "✅" if until and until > now else "❌"
        rem = f", осталось {(until-now)//86400} дн." if until and until > now else ""
        text += f"• `{user_id}` {status} {plan or ''}{rem}\n"
    await update.message.reply_text(text, parse_mode='Markdown')

async def find_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        user = await Database.get_user(user_id)
        if not user:
            await update.message.reply_text("Не найден.")
            return
        filters, until, last_ad, plan = user
        now = int(time.time())
        status = f"✅ активна (осталось {(until-now)//86400} дн.)" if until and until > now else "❌ не активна"
        f_text = json.loads(filters) if filters else "не настроены"
        text = f"**Пользователь {user_id}**\nСтатус: {status}\nПлан: {plan}\nФильтры: {f_text}\nПоследнее объявление: {last_ad}"
        await update.message.reply_text(text, parse_mode='Markdown')
    except:
        await update.message.reply_text("Использование: /find user_id")

async def broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Укажите текст.")
        return
    text = ' '.join(context.args)
    keyboard = [
        [InlineKeyboardButton("✅ Да", callback_data='bc_yes')],
        [InlineKeyboardButton("❌ Нет", callback_data='bc_no')]
    ]
    context.user_data['bc_text'] = text
    await update.message.reply_text(f"Разослать ВСЕМ?\n\n{text}", reply_markup=InlineKeyboardMarkup(keyboard))

async def broadcast_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.from_user.id != ADMIN_ID:
        return
    if q.data == 'bc_yes':
        text = context.user_data.get('bc_text', '')
        if not text:
            await q.edit_message_text("Ошибка.")
            return
        rows = await Database.get_all_users(limit=10000)  # все
        success = 0
        for (user_id, _, _) in rows:
            try:
                await context.bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown')
                success += 1
            except:
                pass
        await q.edit_message_text(f"✅ Рассылка завершена. Успешно: {success}")
    else:
        await q.edit_message_text("Рассылка отменена.")

async def test_parse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.message.reply_text("🔄 Запуск парсинга...")
    # принудительно очистим кэш для этого вызова
    parse_cache.clear()
    users = await Database.get_active_subscribers()
    if users:
        await update.message.reply_text(f"Активных подписчиков: {len(users)}")
        for user_id, filters_json in users:
            filters = json.loads(filters_json)
            districts = filters.get('districts', [])
            metros = filters.get('metros', [])
            ads = await fetch_cian(districts, metros)
            await update.message.reply_text(f"Для {user_id}: найдено {len(ads)} объявлений")
    else:
        await update.message.reply_text("Нет активных подписчиков.")

# ========== ЗАПУСК ==========
async def post_init(app: Application):
    """Запускаем фоновую задачу после инициализации бота."""
    asyncio.create_task(background_parser(app))

def main():
    app = Application.builder().token(TOKEN).post_init(post_init).build()

    # Обработчики
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('status', my_status))
    app.add_handler(CallbackQueryHandler(my_status, pattern='^st$'))

    app.add_handler(CallbackQueryHandler(choose_plan, pattern='^cp$'))
    app.add_handler(CallbackQueryHandler(plan_chosen, pattern='^p\\d+m$'))
    app.add_handler(CallbackQueryHandler(back_to_start, pattern='^bk$'))

    # Фильтры округов
    app.add_handler(CallbackQueryHandler(start_filter, pattern='^fl$'))
    app.add_handler(CallbackQueryHandler(toggle_district, pattern='^d.+$'))
    app.add_handler(CallbackQueryHandler(districts_done, pattern='^dfin$'))

    # Фильтры метро
    app.add_handler(CallbackQueryHandler(metro_line, pattern='^l.+$'))
    app.add_handler(CallbackQueryHandler(toggle_metro, pattern='^m.+$'))
    app.add_handler(CallbackQueryHandler(metro_back, pattern='^mbk$'))
    app.add_handler(CallbackQueryHandler(metros_done, pattern='^mfin$'))

    # Подтверждения оплаты
    app.add_handler(MessageHandler(filters.PHOTO, handle_payment_proof))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_payment_proof))

    # Админские команды
    app.add_handler(CommandHandler('act', activate))
    app.add_handler(CommandHandler('grant', grant))
    app.add_handler(CommandHandler('stats', stats))
    app.add_handler(CommandHandler('users', users_list))
    app.add_handler(CommandHandler('find', find_user))
    app.add_handler(CommandHandler('broadcast', broadcast))
    app.add_handler(CommandHandler('testparse', test_parse))
    app.add_handler(CallbackQueryHandler(broadcast_confirm, pattern='^bc_'))

    logger.info("Бот запускается...")
    app.run_polling()

if __name__ == '__main__':
    asyncio.run(Database.init())
    main()
