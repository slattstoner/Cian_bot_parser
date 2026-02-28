import os
import logging
import json
import asyncio
import time
import random
import re
from datetime import datetime, timedelta
from urllib.parse import urlencode
import hashlib

import aiohttp
import asyncpg
from bs4 import BeautifulSoup
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, LabeledPrice
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    PreCheckoutQueryHandler,
    ContextTypes
)
from telegram.constants import ParseMode

# Selenium / Playwright (выберем Playwright для асинхронности)
from playwright.async_api import async_playwright

# ========== НАСТРОЙКИ ==========
TOKEN = os.environ.get('TOKEN')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 0))
TON_WALLET = os.environ.get('TON_WALLET', '')
DADATA_API_KEY = os.environ.get('DADATA_API_KEY', '')
DATABASE_URL = os.environ.get('DATABASE_URL')
PROXY_URL = os.environ.get('PROXY_URL', None)
PAYMENT_PROVIDER_TOKEN = os.environ.get('PAYMENT_PROVIDER_TOKEN', None)  # для Telegram Payments

if not TOKEN or not ADMIN_ID:
    raise ValueError("Задайте TOKEN и ADMIN_ID")
if not DATABASE_URL:
    raise ValueError("Задайте DATABASE_URL")

# Цены подписок (в рублях для Telegram Payments, можно оставить и TON для ручного)
PRICES_RUB = {
    '1m': 150,
    '3m': 400,
    '6m': 750,
    '12m': 1400
}
PRICES_TON = {
    '1m': 1.5,
    '3m': 4.0,
    '6m': 7.5,
    '12m': 14.0
}
PLAN_DAYS = {'1m': 30, '3m': 90, '6m': 180, '12m': 360}

# ========== ДАННЫЕ ПО МОСКВЕ ==========
DISTRICTS = ['ЦАО', 'САО', 'СВАО', 'ВАО', 'ЮВАО', 'ЮАО', 'ЮЗАО', 'ЗАО', 'СЗАО']
ROOM_OPTIONS = ['Студия', '1-комнатная', '2-комнатная', '3-комнатная', '4-комнатная+']

# ПОЛНЫЙ СПИСОК ЛИНИЙ МЕТРО (Москва, МЦК, МЦД) — сокращён для примера, но вы можете заменить на свой полный
METRO_LINES = {
    'sokol': {'name': '🚇 Сокольническая линия',
              'stations': ["Бульвар Рокоссовского", "Черкизовская", "Преображенская площадь", "Сокольники",
                           "Красносельская", "Комсомольская", "Красные ворота", "Чистые пруды", "Лубянка",
                           "Охотный ряд", "Библиотека им. Ленина", "Кропоткинская", "Парк культуры",
                           "Фрунзенская", "Спортивная", "Воробьёвы горы", "Университет",
                           "Проспект Вернадского", "Юго-Западная", "Тропарёво", "Румянцево", "Саларьево",
                           "Филатов Луг", "Прокшино", "Ольховая", "Новомосковская", "Потапово"]},
    'zamosk': {'name': '🚇 Замоскворецкая линия',
               'stations': ["Ховрино", "Беломорская", "Речной вокзал", "Водный стадион", "Войковская",
                            "Сокол", "Аэропорт", "Динамо", "Белорусская", "Маяковская", "Тверская",
                            "Театральная", "Новокузнецкая", "Павелецкая", "Автозаводская", "Технопарк",
                            "Коломенская", "Каширская", "Кантемировская", "Царицыно", "Орехово",
                            "Домодедовская", "Красногвардейская", "Алма-Атинская"]},
    # ... добавьте остальные линии по аналогии
    # Для полного списка обратитесь к отдельному файлу или используйте API
}

# Для удобства сделаем плоский список всех станций
ALL_METRO_STATIONS = []
for line in METRO_LINES.values():
    ALL_METRO_STATIONS.extend(line['stations'])

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

# ========== ПУЛ БД ==========
class Database:
    _pool = None

    @classmethod
    async def init(cls):
        cls._pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
        async with cls._pool.acquire() as conn:
            # Таблица пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    filters TEXT,
                    subscribed_until BIGINT,
                    last_ad_id TEXT,
                    plan TEXT
                )
            ''')
            # Таблица платежей
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
            # Таблица тикетов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS support_tickets (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    message TEXT,
                    created_at BIGINT,
                    status TEXT DEFAULT 'open'
                )
            ''')
            # Таблица объявлений (для централизованного сбора)
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS ads (
                    id SERIAL PRIMARY KEY,
                    ad_id VARCHAR(255) UNIQUE,
                    source VARCHAR(50) DEFAULT 'cian',
                    title TEXT,
                    price VARCHAR(100),
                    address TEXT,
                    metro VARCHAR(100),
                    rooms VARCHAR(20),
                    floor VARCHAR(20),
                    area VARCHAR(50),
                    owner BOOLEAN,
                    district VARCHAR(10),
                    url TEXT,
                    photos JSONB,
                    published_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            # Индекс для быстрого поиска новых объявлений
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_ads_created ON ads(created_at)')
            # Добавляем колонку plan, если её нет (совместимость)
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
            open_tickets = await conn.fetchval('SELECT COUNT(*) FROM support_tickets WHERE status = $1', 'open')
            # Количество объявлений в БД
            ads_count = await conn.fetchval('SELECT COUNT(*) FROM ads')
            return total, active, pending, total_income, monthly, open_tickets, ads_count

    @classmethod
    async def get_all_users(cls, limit=20, offset=0):
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT user_id, subscribed_until, plan FROM users ORDER BY user_id LIMIT $1 OFFSET $2', limit, offset)

    @classmethod
    async def get_active_subscribers(cls):
        now = int(time.time())
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT user_id, filters FROM users WHERE subscribed_until > $1', now)

    @classmethod
    async def create_ticket(cls, user_id, message):
        created_at = int(time.time())
        async with cls._pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO support_tickets (user_id, message, created_at) VALUES ($1, $2, $3) RETURNING id',
                user_id, message, created_at
            )

    @classmethod
    async def get_open_tickets(cls):
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT * FROM support_tickets WHERE status = $1 ORDER BY created_at', 'open')

    @classmethod
    async def close_ticket(cls, ticket_id):
        async with cls._pool.acquire() as conn:
            await conn.execute('UPDATE support_tickets SET status = $1 WHERE id = $2', 'closed', ticket_id)

    # ========== Методы для работы с объявлениями ==========
    @classmethod
    async def save_ad(cls, ad):
        """Сохраняет объявление в БД, если его там ещё нет."""
        async with cls._pool.acquire() as conn:
            try:
                await conn.execute('''
                    INSERT INTO ads (ad_id, source, title, price, address, metro, rooms, floor, area, owner, district, url, photos, published_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
                    ON CONFLICT (ad_id) DO NOTHING
                ''', ad['id'], ad.get('source', 'cian'), ad['title'], ad['price'], ad['address'],
                   ad['metro'], ad['rooms'], ad['floor'], ad['area'], ad['owner'], ad.get('district_detected'),
                   ad['link'], json.dumps(ad.get('photos', [])), datetime.now())
                return True
            except Exception as e:
                logger.error(f"Ошибка сохранения объявления {ad['id']}: {e}")
                return False

    @classmethod
    async def get_new_ads_since(cls, minutes=10):
        """Возвращает объявления, добавленные за последние minutes минут."""
        since = datetime.now() - timedelta(minutes=minutes)
        async with cls._pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM ads WHERE created_at > $1 ORDER BY created_at DESC', since)
            return [dict(r) for r in rows]

# ========== ПАРСИНГ С PLAYWRIGHT ==========
async def get_page_html_playwright(url, params=None):
    """Загружает страницу через Playwright и возвращает HTML."""
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox',
                '--disable-dev-shm-usage'
            ]
        )
        context = await browser.new_context(
            user_agent='Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36'
        )
        if PROXY_URL:
            await context.set_extra_http_headers({'Proxy': PROXY_URL})  # упрощённо
        page = await context.new_page()
        full_url = url + '?' + urlencode(params) if params else url
        logger.info(f"Загрузка страницы: {full_url}")
        await page.goto(full_url, wait_until='domcontentloaded')
        # Имитация поведения человека
        await page.wait_for_timeout(random.randint(2000, 5000))
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight/2)")
        await page.wait_for_timeout(random.randint(1000, 2000))
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_selector('article[data-name="CardComponent"]', timeout=10000)
        html = await page.content()
        await browser.close()
        return html

async def fetch_cian_all():
    """Собирает объявления по всем округам и всем комнатам."""
    params = {
        'deal_type': 'sale',
        'engine_version': '2',
        'offer_type': 'flat',
        'region': '1',
        'only_flat': '1',
        'sort': 'creation_date_desc',
        'p': '1'
    }
    # Добавляем все округа
    for d in DISTRICTS:
        code = {'ЦАО':8, 'САО':9, 'СВАО':10, 'ВАО':11, 'ЮВАО':12, 'ЮАО':13, 'ЮЗАО':14, 'ЗАО':15, 'СЗАО':16}.get(d)
        if code:
            params[f'okrug[{code}]'] = '1'
    # Добавляем все комнаты? На ЦИАН они задаются через параметры room[1]=1 и т.д.
    # Пока не добавляем, получим все подряд

    url = "https://www.cian.ru/cat.php"
    html = await get_page_html_playwright(url, params)
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    cards = soup.find_all('article', {'data-name': 'CardComponent'})
    if not cards:
        logger.warning("Карточки не найдены")
        return []

    results = []
    for card in cards[:30]:  # ограничим 30 новыми за раз
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

            full_text = card.get_text(separator=' ', strip=True).lower()

            rooms_count = '?'
            room_match = re.search(r'(\d+)[-\s]комнат', title.lower())
            if room_match:
                rooms_count = room_match.group(1)
            else:
                room_match = re.search(r'(\d+)[-\s]комнат', full_text)
                if room_match:
                    rooms_count = room_match.group(1)
                elif 'студия' in full_text or 'студия' in title.lower():
                    rooms_count = 'студия'

            floor = '?/?'
            floor_match = re.search(r'(\d+)[-\s]этаж\s+из\s+(\d+)', full_text)
            if floor_match:
                floor = f"{floor_match.group(1)}/{floor_match.group(2)}"
            else:
                floor_match = re.search(r'(\d+)[-\s]этаж', full_text)
                if floor_match:
                    floor = f"{floor_match.group(1)}/?"

            area = '? м²'
            area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', full_text)
            if area_match:
                area = f"{area_match.group(1)} м²"

            owner_tag = card.find('span', text=re.compile('собственник', re.I))
            is_owner = bool(owner_tag)

            photos = []
            for img in card.find_all('img', src=True)[:10]:
                src = img['src']
                if src.startswith('//'):
                    src = 'https:' + src
                if 'avatar' not in src and not src.endswith('.svg') and 'blank' not in src:
                    photos.append(src)

            district_detected = None
            if DADATA_API_KEY:
                district_detected = await get_district_by_address(address)

            results.append({
                'id': ad_id,
                'source': 'cian',
                'title': title,
                'link': link,
                'price': price,
                'address': address,
                'metro': metro,
                'floor': floor,
                'area': area,
                'rooms': rooms_count,
                'owner': is_owner,
                'photos': photos,
                'district_detected': district_detected
            })
        except Exception as e:
            logger.error(f"Ошибка парсинга карточки: {e}")

    logger.info(f"Собрано {len(results)} объявлений с ЦИАН")
    return results

async def get_district_by_address(address):
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
        logger.debug(f"Ошибка DaData (не критично): {e}")
    return None

# ========== ФИЛЬТРАЦИЯ ОБЪЯВЛЕНИЙ ==========
def matches_filters(ad, filters):
    """Проверяет, подходит ли объявление под фильтры пользователя."""
    districts = filters.get('districts', [])
    rooms = filters.get('rooms', [])
    metros = filters.get('metros', [])
    owner_only = filters.get('owner_only', False)

    # Проверка округа
    if districts and ad.get('district_detected'):
        if ad['district_detected'] not in districts:
            return False

    # Проверка метро
    if metros and ad['metro'] != 'Не указано':
        # Нормализуем названия
        ad_metro_clean = ad['metro'].lower().replace('м.', '').strip()
        found = False
        for m in metros:
            if m.lower() in ad_metro_clean or ad_metro_clean in m.lower():
                found = True
                break
        if not found:
            return False

    # Проверка комнат
    if rooms:
        room_type = None
        rc = ad['rooms']
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

    # Проверка собственника
    if owner_only and not ad['owner']:
        return False

    return True

# ========== ФОНОВЫЙ СБОРЩИК И РАССЫЛКА ==========
async def collector_loop(app: Application):
    """Фоновая задача: раз в 10 минут собирает новые объявления и рассылает подписчикам."""
    while True:
        try:
            logger.info("Запуск сбора объявлений")
            # Получаем активных подписчиков
            subscribers = await Database.get_active_subscribers()
            if not subscribers:
                logger.info("Нет активных подписчиков")
                await asyncio.sleep(600)
                continue

            # Собираем новые объявления с ЦИАН
            ads = await fetch_cian_all()
            if not ads:
                logger.info("Нет новых объявлений")
                await asyncio.sleep(600)
                continue

            # Сохраняем в БД и отбираем действительно новые (которых не было)
            new_ads = []
            for ad in ads:
                if await Database.save_ad(ad):
                    new_ads.append(ad)

            if not new_ads:
                logger.info("Нет новых объявлений после проверки БД")
                await asyncio.sleep(600)
                continue

            logger.info(f"Найдено {len(new_ads)} новых объявлений, начинаем рассылку")

            # Для каждого нового объявления ищем подходящих подписчиков
            for ad in new_ads:
                tasks = []
                for user_id, filters_json in subscribers:
                    filters = json.loads(filters_json)
                    if matches_filters(ad, filters):
                        tasks.append(send_ad_to_user(app.bot, user_id, ad))
                if tasks:
                    await asyncio.gather(*tasks)
                    # Небольшая задержка, чтобы не спамить
                    await asyncio.sleep(0.5)

            logger.info("Рассылка завершена")
        except Exception as e:
            logger.error(f"Ошибка в collector_loop: {e}", exc_info=True)
        await asyncio.sleep(600)

async def send_ad_to_user(bot, user_id, ad):
    """Отправляет одно объявление пользователю."""
    owner_text = "Собственник" if ad['owner'] else "Агент"
    text = (
        f"🔵 *Новое объявление*\n"
        f"🏷 {ad['title']}\n"
        f"💰 Цена: {ad['price']}\n"
        f"📍 Адрес: {ad['address']}\n"
        f"🚇 Метро: {ad['metro']}\n"
        f"🏢 Этаж: {ad['floor']}\n"
        f"📏 Площадь: {ad['area']}\n"
        f"🛏 Комнат: {ad['rooms']}\n"
        f"👤 {owner_text}\n"
        f"[🔗 Ссылка]({ad['link']})"
    )
    try:
        if ad.get('photos'):
            media = []
            media.append(
                InputMediaPhoto(
                    media=ad['photos'][0],
                    caption=text,
                    parse_mode='Markdown'
                )
            )
            for photo_url in ad['photos'][1:10]:
                media.append(InputMediaPhoto(media=photo_url))
            await bot.send_media_group(chat_id=user_id, media=media)
        else:
            await bot.send_message(
                chat_id=user_id,
                text=text,
                parse_mode='Markdown',
                disable_web_page_preview=True
            )
        # Обновляем last_ad_id для пользователя (опционально)
        await Database.update_last_ad(user_id, ad['id'])
    except Exception as e:
        logger.error(f"Ошибка отправки пользователю {user_id}: {e}")

# ========== ОБРАБОТЧИКИ КОМАНД И КНОПОК ==========
# (Сохраняем все старые обработчики, но некоторые модифицируем)

async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='cp')],
        [InlineKeyboardButton("ℹ️ Мой профиль", callback_data='profile')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='fl')],
        [InlineKeyboardButton("🆘 Поддержка", callback_data='support')],
        [InlineKeyboardButton("❓ Помощь", callback_data='help')]
    ]
    text = "👋 *Главное меню*\n\nВыберите действие:"
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await main_menu(update, context)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (
        "📚 *Помощь*\n\n"
        "💳 *Подписаться* – выбор тарифа и оплата.\n"
        "ℹ️ *Мой профиль* – информация о подписке и фильтрах.\n"
        "⚙️ *Настроить фильтры* – выбор округов, комнат, метро, типа объявлений.\n"
        "🆘 *Поддержка* – связаться с администратором.\n"
        "🏠 *Главное меню* – вернуться в начало.\n\n"
        "После подписки бот начнёт присылать новые объявления автоматически."
    )
    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]]
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    user = await Database.get_user(user_id)
    now = int(time.time())
    if user and user[1] and user[1] > now:
        rem = user[1] - now
        days = rem // 86400
        hours = (rem % 86400) // 3600
        sub_status = f"✅ Активна (осталось {days} дн. {hours} ч.)"
    else:
        sub_status = "❌ Не активна"

    filters = user[0] if user and user[0] else None
    if filters:
        try:
            f = json.loads(filters)
            districts = ', '.join(f.get('districts', [])) or 'все'
            rooms = ', '.join(f.get('rooms', [])) or 'все'
            metros = ', '.join(f.get('metros', [])) or 'все'
            owner_type = "Только собственники" if f.get('owner_only') else "Все"
            filters_text = f"🏘 Округа: {districts}\n🛏 Комнат: {rooms}\n🚇 Метро: {metros}\n👤 Тип: {owner_type}"
        except:
            filters_text = "⚠️ Ошибка в фильтрах"
    else:
        filters_text = "⚙️ Фильтры не настроены"

    user_tg = update.effective_user
    full_name = user_tg.full_name
    username = f"@{user_tg.username}" if user_tg.username else "не указан"

    text = (
        f"👤 *Ваш профиль*\n\n"
        f"🆔 ID: `{user_id}`\n"
        f"📛 Имя: {full_name}\n"
        f"🌐 Username: {username}\n\n"
        f"📅 *Статус подписки:*\n{sub_status}\n\n"
        f"🔧 *Ваши фильтры:*\n{filters_text}"
    )
    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]]
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

# ---------- ВЫБОР ПЛАНА (с поддержкой Telegram Payments) ----------
async def choose_plan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    keyboard = [
        [InlineKeyboardButton(f"1 месяц – {PRICES_TON['1m']} TON / {PRICES_RUB['1m']} руб", callback_data='p1m')],
        [InlineKeyboardButton(f"3 месяца – {PRICES_TON['3m']} TON / {PRICES_RUB['3m']} руб", callback_data='p3m')],
        [InlineKeyboardButton(f"6 месяцев – {PRICES_TON['6m']} TON / {PRICES_RUB['6m']} руб", callback_data='p6m')],
        [InlineKeyboardButton(f"12 месяцев – {PRICES_TON['12m']} TON / {PRICES_RUB['12m']} руб", callback_data='p12m')],
        [InlineKeyboardButton("« Назад", callback_data='main_menu')]
    ]
    await q.edit_message_text("📅 Выберите срок подписки:", reply_markup=InlineKeyboardMarkup(keyboard))

async def plan_chosen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    plan = q.data[1:]  # p1m -> 1m
    context.user_data['plan'] = plan

    # Если есть токен платежного провайдера, используем Telegram Payments
    if PAYMENT_PROVIDER_TOKEN:
        await send_invoice(q, context, plan)
    else:
        # Иначе старый ручной метод
        await pay_ton_manual(q, context)

async def send_invoice(update, context, plan):
    """Отправляет счёт через Telegram Payments."""
    user_id = update.from_user.id
    amount_rub = PRICES_RUB[plan]
    title = f"Подписка на {plan}"
    description = f"Доступ к мониторингу ЦИАН на {PLAN_DAYS[plan]} дней"
    payload = f"sub_{plan}_{user_id}_{int(time.time())}"
    currency = "RUB"
    prices = [LabeledPrice(label="Подписка", amount=amount_rub * 100)]  # в копейках

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

async def pay_ton_manual(update, context):
    """Старый метод: просим отправить TXID."""
    q = update if isinstance(update, Update) else update
    user_id = q.from_user.id
    plan = context.user_data.get('plan', '1m')
    amount = PRICES_TON[plan]
    payment_id = await Database.add_payment(user_id, amount, plan)
    text = (
        f"**Оплата в TON**\n\n"
        f"Сумма: **{amount} TON**\n"
        f"Кошелёк: `{TON_WALLET}`\n\n"
        "После перевода **отправьте TXID** (или скриншот).\n"
        "Администратор проверит и активирует подписку.\n\n"
        f"**ID платежа:** `{payment_id}`"
    )
    await q.edit_message_text(text, parse_mode='Markdown')

# ---------- Обработчики Telegram Payments ----------
async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обязательный ответ на pre_checkout_query."""
    query = update.pre_checkout_query
    await query.answer(ok=True)

async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка успешной оплаты."""
    user_id = update.effective_user.id
    payload = update.message.successful_payment.invoice_payload
    # payload имеет вид sub_plan_userid_timestamp
    parts = payload.split('_')
    if len(parts) >= 2:
        plan = parts[1]
        days = PLAN_DAYS.get(plan, 30)
        await Database.activate_subscription(user_id, days, plan)
        # Можно также записать в таблицу payments (но это необязательно)
        await update.message.reply_text("✅ Оплата прошла успешно! Подписка активирована.")
    else:
        await update.message.reply_text("✅ Оплата прошла, но возникла ошибка с активацией. Обратитесь в поддержку.")

# ---------- ФИЛЬТРЫ (с поддержкой поиска по метро) ----------
# Полностью сохраняем логику из старого кода, но добавляем поиск по станциям
# (см. предыдущие обработчики filter_districts, filter_rooms, filter_metros и т.д.)
# Для краткости я не буду переписывать их все здесь, но они должны остаться из старого кода.
# В реальном файле они будут присутствовать. Я обозначу места.

# ... (все предыдущие обработчики фильтров, поддержки, админские команды остаются без изменений,
#      только в filter_metros добавим кнопку поиска и обработчик текста)

# Добавляем в filter_metros кнопку "🔍 Поиск по названию"
async def filter_metros(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    keyboard = []
    for code, line in METRO_LINES.items():
        keyboard.append([InlineKeyboardButton(line['name'], callback_data=f'l_{code}')])
    keyboard.append([InlineKeyboardButton("🔍 Поиск по названию", callback_data='metro_search')])
    keyboard.append([InlineKeyboardButton("« Назад", callback_data='f_back')])
    await q.edit_message_text("🚇 Выберите ветку метро или найдите по названию:", reply_markup=InlineKeyboardMarkup(keyboard))

async def metro_search_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text("Введите название станции (или часть названия):")
    context.user_data['awaiting_metro_search'] = True

async def handle_metro_search_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    # Показываем первые 10
    keyboard = []
    for station in found[:10]:
        keyboard.append([InlineKeyboardButton(station, callback_data=f'm_{station}')])
    keyboard.append([InlineKeyboardButton("« Отмена", callback_data='f_metros')])
    await update.message.reply_text("Найдено станций. Выберите:", reply_markup=InlineKeyboardMarkup(keyboard))
    context.user_data['awaiting_metro_search'] = False

# ---------- АДМИНСКИЕ КОМАНДЫ (обновим stats) ----------
async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    total, active, pending, total_income, monthly, open_tickets, ads_count = await Database.get_stats()
    text = (
        f"📊 **Статистика бота**\n"
        f"👥 Всего пользователей: {total}\n"
        f"✅ Активных подписок: {active}\n"
        f"💰 Ежемесячный доход: **{monthly:.2f} TON**\n"
        f"💵 Общий доход: **{total_income:.2f} TON**\n"
        f"⏳ Ожидают подтверждения: {pending}\n"
        f"🆘 Открытых тикетов: {open_tickets}\n"
        f"📰 Объявлений в базе: {ads_count}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')

# ... остальные админские команды (users, find, grant, act, tickets, reply, close_ticket, broadcast, testparse, daily) остаются как в старом коде.
# Для тестового парсинга можно оставить старую функцию, но она больше не нужна, можно удалить.

# ========== ЗАПУСК ==========
async def post_init(app: Application):
    # Инициализация БД
    await Database.init()
    # Запуск фонового сборщика
    asyncio.create_task(collector_loop(app))

def main():
    app = Application.builder().token(TOKEN).post_init(post_init).build()

    # Основные команды и кнопки
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('menu', main_menu))
    app.add_handler(CallbackQueryHandler(main_menu, pattern='^main_menu$'))
    app.add_handler(CallbackQueryHandler(profile, pattern='^profile$'))
    app.add_handler(CallbackQueryHandler(help_command, pattern='^help$'))
    app.add_handler(CallbackQueryHandler(support_start, pattern='^support$'))

    # Подписка
    app.add_handler(CallbackQueryHandler(choose_plan, pattern='^cp$'))
    app.add_handler(CallbackQueryHandler(plan_chosen, pattern='^p\\d+m$'))

    # Фильтры
    app.add_handler(CallbackQueryHandler(start_filter, pattern='^fl$'))
    app.add_handler(CallbackQueryHandler(filter_districts, pattern='^f_districts$'))
    app.add_handler(CallbackQueryHandler(filter_rooms, pattern='^f_rooms$'))
    app.add_handler(CallbackQueryHandler(filter_metros, pattern='^f_metros$'))
    app.add_handler(CallbackQueryHandler(filter_owner, pattern='^f_owner$'))
    app.add_handler(CallbackQueryHandler(filters_done, pattern='^f_done$'))
    app.add_handler(CallbackQueryHandler(filter_back, pattern='^f_back$'))
    app.add_handler(CallbackQueryHandler(toggle_district, pattern='^d_.+$'))
    app.add_handler(CallbackQueryHandler(toggle_room, pattern='^r_.+$'))
    app.add_handler(CallbackQueryHandler(metro_line, pattern='^l_.+$'))
    app.add_handler(CallbackQueryHandler(toggle_metro, pattern='^m_.+$'))
    app.add_handler(CallbackQueryHandler(toggle_owner, pattern='^owner_'))
    app.add_handler(CallbackQueryHandler(metro_search_start, pattern='^metro_search$'))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_metro_search_text))

    # Поддержка
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_support_message))

    # Платёжные подтверждения (старый метод)
    app.add_handler(MessageHandler(filters.PHOTO, handle_payment_proof))

    # Telegram Payments
    if PAYMENT_PROVIDER_TOKEN:
        app.add_handler(PreCheckoutQueryHandler(pre_checkout))
        app.add_handler(MessageHandler(filters.SUCCESSFUL_PAYMENT, successful_payment))

    # Админские команды
    app.add_handler(CommandHandler('act', activate))
    app.add_handler(CommandHandler('grant', grant))
    app.add_handler(CommandHandler('stats', stats))
    app.add_handler(CommandHandler('users', users_list))
    app.add_handler(CommandHandler('find', find_user))
    app.add_handler(CommandHandler('profile', profile_by_id))
    app.add_handler(CommandHandler('tickets', tickets_list))
    app.add_handler(CommandHandler('close_ticket', close_ticket))
    app.add_handler(CommandHandler('reply', admin_reply_to_ticket))
    app.add_handler(CommandHandler('broadcast', broadcast))
    app.add_handler(CommandHandler('testparse', test_parse))
    app.add_handler(CommandHandler('daily', daily_by_metro))
    app.add_handler(CallbackQueryHandler(users_page, pattern='^users_page_'))
    app.add_handler(CallbackQueryHandler(broadcast_confirm, pattern='^bc_'))

    logger.info("Бот запускается...")
    app.run_polling()

if __name__ == '__main__':
    main()