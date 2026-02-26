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
from fake_useragent import UserAgent
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
PROXY_URL = os.environ.get('PROXY_URL', None)  # опционально

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
ROOM_OPTIONS = ['Студия', '1-комнатная', '2-комнатная', '3-комнатная', '4-комнатная+']
OWNER_TYPES = ['Все', 'Только собственники']  # для фильтра

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

# ========== ФУНКЦИИ ДЛЯ ОБХОДА БЛОКИРОВОК ==========
ua = UserAgent()

async def make_request(url, headers=None, params=None, retries=3):
    """Выполняет HTTP-запрос с ротацией User-Agent, поддержкой прокси и повторными попытками."""
    if headers is None:
        headers = {}
    headers['User-Agent'] = ua.random
    headers['Accept-Language'] = 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7'
    headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    headers['Connection'] = 'keep-alive'
    headers['Upgrade-Insecure-Requests'] = '1'

    connector = aiohttp.TCPConnector(ssl=False)
    proxy = PROXY_URL if PROXY_URL else None

    for attempt in range(retries):
        try:
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.get(url, params=params, headers=headers, proxy=proxy, timeout=30) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    else:
                        logger.warning(f"Попытка {attempt+1}: статус {resp.status}")
        except Exception as e:
            logger.warning(f"Попытка {attempt+1} не удалась: {e}")
        await asyncio.sleep((attempt + 1) * random.uniform(2, 5))
    return None

# ========== КЭШ ПАРСИНГА ==========
parse_cache = {}  # key: tuple(...) -> (data, expiry)

def cache_key(districts, rooms, metros, owner_only):
    return (tuple(sorted(districts)), tuple(sorted(rooms)), tuple(sorted(metros)), owner_only)

async def fetch_cian(districts, rooms, metros, owner_only):
    """Асинхронный парсинг ЦИАН с учётом всех фильтров."""
    key = cache_key(districts, rooms, metros, owner_only)
    now = time.time()
    if key in parse_cache and parse_cache[key][1] > now:
        logger.info("Использую кэш парсинга")
        return parse_cache[key][0]

    params = {
        'deal_type': 'sale',
        'engine_version': '2',
        'offer_type': 'flat',
        'region': '1',
        'only_flat': '1',
        'owner': '1' if owner_only else None,  # если только собственники, ставим owner=1, иначе убираем
        'sort': 'creation_date_desc',
        'p': '1'
    }
    # Убираем None значения
    params = {k: v for k, v in params.items() if v is not None}

    # Добавляем округа
    for d in districts:
        code = {'ЦАО':8, 'САО':9, 'СВАО':10, 'ВАО':11, 'ЮВАО':12, 'ЮАО':13, 'ЮЗАО':14, 'ЗАО':15, 'СЗАО':16}.get(d)
        if code:
            params[f'okrug[{code}]'] = '1'

    url = "https://www.cian.ru/cat.php"
    logger.info(f"Парсинг: {url} с параметрами {params}")

    html = await make_request(url, params=params)
    if not html:
        logger.error("Не удалось получить HTML после нескольких попыток")
        return []

    # Диагностика: сохраняем начало страницы в лог
    logger.info(f"Первые 2000 символов ответа: {html[:2000]}")

    soup = BeautifulSoup(html, 'lxml')

    # Универсальный поиск карточек объявлений
    cards = []
    # Пробуем разные селекторы
    selectors = [
        ('article', {'data-name': 'CardComponent'}),
        ('div', class_=re.compile('_93444fe79c--card--')),
        ('div', {'data-testid': 'offer-card'}),
        ('article', {'class': re.compile('offer-card')}),
        ('div', {'class': 'catalog-offers'})
    ]
    for tag, attrs in selectors:
        found = soup.find_all(tag, attrs)
        if found:
            logger.info(f"Найдено карточек по селектору {tag}:{attrs} - {len(found)}")
            cards = found
            break
    else:
        # Если ничего не нашли, пробуем найти любые div с ценой
        all_divs = soup.find_all('div', class_=re.compile('offer|card|item|container'))
        logger.info(f"Ничего не найдено, всего div'ов с offer/card: {len(all_divs)}")
        return []

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

            # Цена
            price_tag = card.find('span', {'data-mark': 'MainPrice'}) or card.find('span', class_=re.compile('price'))
            price = price_tag.text.strip() if price_tag else 'Цена не указана'

            # Адрес
            address_tag = card.find('address') or card.find('span', class_=re.compile('address'))
            address = address_tag.text.strip() if address_tag else 'Москва'

            # Метро
            metro_tag = card.find('span', class_=re.compile('underground')) or card.find('a', href=re.compile('metro'))
            metro = metro_tag.text.strip() if metro_tag else 'Не указано'

            # Заголовок
            title_tag = card.find('h3')
            title = title_tag.text.strip() if title_tag else 'Квартира'

            # Комнаты
            rooms_count = '?'
            room_match = re.search(r'(\d+)[-\s]комнат', title.lower())
            if room_match:
                rooms_count = room_match.group(1)
            else:
                chars = card.find_all('span', class_=re.compile('characteristic'))
                chars_text = ' '.join(c.text for c in chars)
                room_match = re.search(r'(\d+)[-\s]комнат', chars_text.lower())
                if room_match:
                    rooms_count = room_match.group(1)
                elif 'студия' in title.lower() or 'студия' in chars_text.lower():
                    rooms_count = 'студия'

            # Проверка фильтра по комнатам
            if rooms:
                room_type = None
                if rooms_count == 'студия':
                    room_type = 'Студия'
                elif rooms_count == '1':
                    room_type = '1-комнатная'
                elif rooms_count == '2':
                    room_type = '2-комнатная'
                elif rooms_count == '3':
                    room_type = '3-комнатная'
                elif rooms_count == '4' or (rooms_count.isdigit() and int(rooms_count) >= 4):
                    room_type = '4-комнатная+'
                if room_type not in rooms:
                    continue

            # Характеристики (этаж, площадь)
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

            # Определение собственника
            owner_tag = card.find('span', text=re.compile('собственник', re.I))
            is_owner = bool(owner_tag)
            # Если фильтр "только собственники" и это не собственник, пропускаем
            if owner_only and not is_owner:
                continue

            # Фото
            photos = []
            for img in card.find_all('img', src=True)[:3]:
                src = img['src']
                if src.startswith('//'):
                    src = 'https:' + src
                if 'avatar' not in src and not src.endswith('.svg'):
                    photos.append(src)

            # Округ через DaData (опционально)
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
                'rooms': rooms_count,
                'owner': is_owner,
                'photos': photos,
                'district_detected': district_detected
            })
        except Exception as e:
            logger.error(f"Ошибка парсинга карточки: {e}")

    parse_cache[key] = (results, now + 300)  # кэш на 5 минут
    logger.info(f"Успешно распарсено {len(results)} объявлений")
    return results

async def fetch_daily_by_metro(metro_stations=None):
    """
    Парсит свежие объявления (1 страница) и возвращает только те,
    которые привязаны к указанным станциям метро.
    Если metro_stations = None или пусто, возвращает все.
    """
    params = {
        'deal_type': 'sale',
        'engine_version': '2',
        'offer_type': 'flat',
        'region': '1',
        'only_flat': '1',
        'owner': '1',  # для daily тоже можно искать только собственников? пока оставим всех
        'sort': 'creation_date_desc',
        'p': '1'
    }
    url = "https://www.cian.ru/cat.php"
    logger.info(f"Ежедневный парсинг по метро: {url}")

    html = await make_request(url, params=params)
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    # Поиск карточек аналогично основному парсеру
    cards = []
    selectors = [
        ('article', {'data-name': 'CardComponent'}),
        ('div', class_=re.compile('_93444fe79c--card--')),
        ('div', {'data-testid': 'offer-card'}),
        ('article', {'class': re.compile('offer-card')}),
        ('div', {'class': 'catalog-offers'})
    ]
    for tag, attrs in selectors:
        found = soup.find_all(tag, attrs)
        if found:
            cards = found
            break
    if not cards:
        return []

    results = []
    for card in cards[:20]:
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

            rooms_count = '?'
            room_match = re.search(r'(\d+)[-\s]комнат', title.lower())
            if room_match:
                rooms_count = room_match.group(1)
            else:
                chars = card.find_all('span', class_=re.compile('characteristic'))
                chars_text = ' '.join(c.text for c in chars)
                room_match = re.search(r'(\d+)[-\s]комнат', chars_text.lower())
                if room_match:
                    rooms_count = room_match.group(1)
                elif 'студия' in title.lower() or 'студия' in chars_text.lower():
                    rooms_count = 'студия'

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

            is_owner = bool(card.find('span', text=re.compile('собственник', re.I)))

            photos = []
            for img in card.find_all('img', src=True)[:3]:
                src = img['src']
                if src.startswith('//'):
                    src = 'https:' + src
                if 'avatar' not in src and not src.endswith('.svg'):
                    photos.append(src)

            # Фильтр по станциям метро
            if metro_stations and metro != 'Не указано':
                metro_clean = metro.lower().replace('м.', '').strip()
                match = False
                for st in metro_stations:
                    if st.lower() in metro_clean or metro_clean in st.lower():
                        match = True
                        break
                if not match:
                    continue

            results.append({
                'id': ad_id,
                'title': title,
                'link': link,
                'price': price,
                'address': address,
                'metro': metro,
                'floor': floor,
                'area': area,
                'rooms': rooms_count,
                'owner': is_owner,
                'photos': photos
            })
        except Exception as e:
            logger.error(f"Ошибка парсинга карточки: {e}")

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
                rooms = filters.get('rooms', [])
                metros = filters.get('metros', [])
                owner_only = filters.get('owner_only', False)  # по умолчанию False (все)
                ads = await fetch_cian(districts, rooms, metros, owner_only)
                if not ads:
                    continue

                user_data = await Database.get_user(user_id)
                last_ad_id = user_data[2] if user_data else None
                new_ads = [a for a in ads if a['id'] != last_ad_id]

                for ad in new_ads[:3]:
                    # Проверка по округу (если задан)
                    district_ok = True
                    if districts and ad.get('district_detected'):
                        district_ok = ad['district_detected'] in districts
                    # По метро
                    metro_ok = True
                    if metros and ad['metro'] != 'Не указано':
                        metro_ok = ad['metro'] in metros
                    # По комнатам уже отфильтровано в fetch_cian, но на всякий случай проверим
                    room_ok = True
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
                        room_ok = (room_type in rooms) if room_type else False

                    # owner_only уже учтён в fetch_cian, но дополнительная проверка
                    owner_ok = True
                    if owner_only:
                        owner_ok = ad['owner']  # должно быть True, иначе не попало бы в ads

                    if (not districts and not metros and not rooms) or (district_ok and metro_ok and room_ok and owner_ok):
                        owner_text = "Собственник" if ad['owner'] else "Агент"
                        text = (
                            f"🔵 *Новое объявление*\n{ad['title']}\n"
                            f"💰 Цена: {ad['price']}\n📍 Адрес: {ad['address']}\n"
                            f"🚇 Метро: {ad['metro']}\n🏢 Этаж: {ad['floor']}\n"
                            f"📏 Площадь: {ad['area']}\n🛏 Комнат: {ad['rooms']}\n"
                            f"👤 {owner_text}\n[Ссылка]({ad['link']})"
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
        "👋 Добро пожаловать в бот для поиска свежих объявлений о квартирах!\n\n"
        "🔍 Я отслеживаю новые объявления от собственников на ЦИАН (Москва) и присылаю их вам сразу после публикации. "
        "Вы сможете первыми увидеть интересные варианты и вовремя на них отреагировать.\n\n"
        "📦 В каждом сообщении: ссылка, цена, адрес, метро, этаж, площадь, количество комнат, пометка «Собственник» или «Агент», фото.\n\n"
        "⚙️ Чтобы начать, оформите подписку и настройте фильтры (округа, количество комнат, станции метро, тип объявления).\n\n"
        "💎 Оплата принимается в **TON**."
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
            rooms = ', '.join(f.get('rooms', [])) or 'все'
            metros = ', '.join(f.get('metros', [])) or 'все'
            owner_type = "Только собственники" if f.get('owner_only') else "Все"
            disp = (f"🏙 **Город:** {city}\n"
                    f"🏘 **Округа:** {districts}\n"
                    f"🛏 **Комнат:** {rooms}\n"
                    f"🚇 **Метро:** {metros}\n"
                    f"👤 **Тип:** {owner_type}")
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
        "Администратор проверит и активирует подписку.\n\n"
        f"**ID платежа:** `{payment_id}`"
    )
    await q.edit_message_text(text, parse_mode='Markdown')

# ---------- ФИЛЬТРЫ ----------
async def start_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data['districts'] = []
    context.user_data['rooms'] = []
    context.user_data['metros'] = []
    context.user_data['owner_only'] = False  # по умолчанию все
    keyboard = [
        [InlineKeyboardButton("🏘 Выбрать округа", callback_data='f_districts')],
        [InlineKeyboardButton("🛏 Выбрать комнаты", callback_data='f_rooms')],
        [InlineKeyboardButton("🚇 Выбрать метро", callback_data='f_metros')],
        [InlineKeyboardButton("👤 Выбрать тип", callback_data='f_owner')],
        [InlineKeyboardButton("✅ Завершить настройку", callback_data='f_done')]
    ]
    await q.edit_message_text("⚙️ **Настройка фильтров**\nВыберите, что хотите настроить:", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

# --- Выбор округов ---
async def filter_districts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    selected = context.user_data.get('districts', [])
    keyboard = []
    for d in DISTRICTS:
        mark = "✅" if d in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {d}", callback_data=f'd_{d}')])
    keyboard.append([InlineKeyboardButton("« Назад", callback_data='f_back')])
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
    keyboard.append([InlineKeyboardButton("« Назад", callback_data='f_back')])
    await q.edit_message_text("🏘 Выберите округа:", reply_markup=InlineKeyboardMarkup(keyboard))

# --- Выбор комнат ---
async def filter_rooms(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    selected = context.user_data.get('rooms', [])
    keyboard = []
    for r in ROOM_OPTIONS:
        mark = "✅" if r in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {r}", callback_data=f'r_{r}')])
    keyboard.append([InlineKeyboardButton("« Назад", callback_data='f_back')])
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
    keyboard.append([InlineKeyboardButton("« Назад", callback_data='f_back')])
    await q.edit_message_text("🛏 Выберите количество комнат:", reply_markup=InlineKeyboardMarkup(keyboard))

# --- Выбор метро ---
async def filter_metros(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    keyboard = []
    for code, line in METRO_LINES.items():
        keyboard.append([InlineKeyboardButton(line['name'], callback_data=f'l_{code}')])
    keyboard.append([InlineKeyboardButton("« Назад", callback_data='f_back')])
    await q.edit_message_text("🚇 Выберите ветку метро:", reply_markup=InlineKeyboardMarkup(keyboard))

async def metro_line(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    line_code = q.data[2:]
    context.user_data['cur_line'] = line_code
    line = METRO_LINES[line_code]
    selected = context.user_data.get('metros', [])
    keyboard = []
    for s in line['stations']:
        mark = "✅" if s in selected else "⬜"
        keyboard.append([InlineKeyboardButton(f"{mark} {s}", callback_data=f'm_{s}')])
    keyboard.append([InlineKeyboardButton("« Назад к веткам", callback_data='f_metros')])
    await q.edit_message_text(f"🚇 **{line['name']}**\nВыберите станции:", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def toggle_metro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    station = q.data[2:]
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
        keyboard.append([InlineKeyboardButton(f"{mark} {s}", callback_data=f'm_{s}')])
    keyboard.append([InlineKeyboardButton("« Назад к веткам", callback_data='f_metros')])
    await q.edit_message_text(f"🚇 **{line['name']}**\nВыберите станции:", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

# --- Выбор типа (собственник/все) ---
async def filter_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    current = context.user_data.get('owner_only', False)
    # current = False (все), True (только собственники)
    text = "👤 Выберите тип объявлений:\n"
    keyboard = [
        [InlineKeyboardButton("✅ Все (агенты и собственники)" if not current else "⬜ Все (агенты и собственники)", callback_data='owner_all')],
        [InlineKeyboardButton("✅ Только собственники" if current else "⬜ Только собственники", callback_data='owner_only')],
        [InlineKeyboardButton("« Назад", callback_data='f_back')]
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

async def toggle_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    if q.data == 'owner_all':
        context.user_data['owner_only'] = False
    elif q.data == 'owner_only':
        context.user_data['owner_only'] = True
    # Обновляем отображение
    current = context.user_data.get('owner_only', False)
    text = "👤 Выберите тип объявлений:\n"
    keyboard = [
        [InlineKeyboardButton("✅ Все (агенты и собственники)" if not current else "⬜ Все (агенты и собственники)", callback_data='owner_all')],
        [InlineKeyboardButton("✅ Только собственники" if current else "⬜ Только собственники", callback_data='owner_only')],
        [InlineKeyboardButton("« Назад", callback_data='f_back')]
    ]
    await q.edit_message_text(text, reply_markup=InlineKeyboardMarkup(keyboard))

# --- Назад в меню фильтров ---
async def filter_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await start_filter(update, context)

# --- Завершение настройки и сохранение ---
async def filters_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    user_id = q.from_user.id
    districts = context.user_data.get('districts', [])
    rooms = context.user_data.get('rooms', [])
    metros = context.user_data.get('metros', [])
    owner_only = context.user_data.get('owner_only', False)
    filters = {
        'city': 'Москва',
        'districts': districts,
        'rooms': rooms,
        'metros': metros,
        'owner_only': owner_only
    }
    await Database.set_user_filters(user_id, filters)

    text = "✅ **Фильтры сохранены!**\n\n🏙 Город: Москва\n"
    text += f"🏘 Округа: {', '.join(districts) if districts else 'все'}\n"
    text += f"🛏 Комнат: {', '.join(rooms) if rooms else 'все'}\n"
    text += f"🚇 Метро: {', '.join(metros) if metros else 'все'}\n"
    text += f"👤 Тип: {'Только собственники' if owner_only else 'Все'}"
    await q.edit_message_text(text, parse_mode='Markdown')
    # Возвращаем в главное меню
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
    welcome = (
        "👋 Добро пожаловать в бот для поиска свежих объявлений о квартирах!\n\n"
        "🔍 Я отслеживаю новые объявления от собственников на ЦИАН (Москва) и присылаю их вам сразу после публикации.\n\n"
        "💎 Оплата принимается в **TON**."
    )
    await q.edit_message_text(welcome, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

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

# ---------- КОМАНДА ДЛЯ ПРОВЕРКИ ПО МЕТРО ----------
async def daily_by_metro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Эта команда только для администратора.")
        return
    args = context.args
    if not args:
        await update.message.reply_text(
            "ℹ️ Использование: /daily станция1 станция2 ...\nПример: /daily Арбатская"
        )
        return
    stations_text = ' '.join(args)
    stations = [s.strip() for s in stations_text.split() if s.strip()]
    await update.message.reply_text(f"🔄 Ищу свежие объявления по станциям: {', '.join(stations)}...")
    logger.info(f"Поиск по станциям: {stations}")

    try:
        ads = await fetch_daily_by_metro(stations)
        if not ads:
            await update.message.reply_text("❌ Объявлений не найдено.")
            return
        await update.message.reply_text(f"✅ Найдено объявлений: {len(ads)}")
        for ad in ads[:5]:
            owner = "Собственник" if ad['owner'] else "Агент"
            text = (
                f"🔵 *{ad['title']}*\n"
                f"💰 Цена: {ad['price']}\n📍 Адрес: {ad['address']}\n"
                f"🚇 Метро: {ad['metro']}\n🏢 Этаж: {ad['floor']}\n"
                f"📏 Площадь: {ad['area']}\n🛏 Комнат: {ad['rooms']}\n"
                f"👤 {owner}\n[Ссылка]({ad['link']})"
            )
            await update.message.reply_text(text, parse_mode='Markdown', disable_web_page_preview=True)
            if ad['photos']:
                for photo in ad['photos'][:3]:
                    await context.bot.send_photo(chat_id=update.effective_user.id, photo=photo)
            await asyncio.sleep(0.5)
        if len(ads) > 5:
            await update.message.reply_text(f"... и ещё {len(ads)-5} объявлений.")
    except Exception as e:
        error_msg = f"❌ Ошибка: {type(e).__name__}: {e}"
        await update.message.reply_text(error_msg)
        logger.exception("Ошибка в daily_by_metro")

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
            await context.bot.send_message(chat_id=user_id, text="✅ Ваша подписка активирована! Настройте фильтры.")
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
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}. Использование: /grant user_id days [plan]")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    total, active, pending, total_income, monthly = await Database.get_stats()
    text = (
        f"📊 **Статистика**\n👥 Всего: {total}\n✅ Активных: {active}\n"
        f"💰 Ежемесячный доход: **{monthly:.2f} TON**\n💵 Общий доход: **{total_income:.2f} TON**\n"
        f"⏳ Ожидают подтверждения: {pending}"
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
        if until and until > now:
            remaining = (until - now) // 86400
            status = f"✅ (осталось {remaining} дн.)"
        else:
            status = "❌ не активна"
        text += f"• `{user_id}` {status} {plan or ''}\n"
    await update.message.reply_text(text, parse_mode='Markdown')

async def find_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        user = await Database.get_user(user_id)
        if not user:
            await update.message.reply_text("Пользователь не найден.")
            return
        filters, until, last_ad, plan = user
        now = int(time.time())
        status = f"✅ активна (осталось {(until-now)//86400} дн.)" if until and until > now else "❌ не активна"
        f_text = json.loads(filters) if filters else "не настроены"
        text = f"**Пользователь {user_id}**\nСтатус: {status}\nПлан: {plan}\nФильтры: {f_text}\nПоследнее объявление: {last_ad}"
        await update.message.reply_text(text, parse_mode='Markdown')
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /find user_id")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

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
        rows = await Database.get_all_users(limit=10000)
        success = 0
        for (user_id, _, _) in rows:
            try:
                await context.bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown')
                success += 1
            except Exception as e:
                logger.error(f"Ошибка отправки {user_id}: {e}")
        await q.edit_message_text(f"✅ Рассылка завершена. Успешно: {success}")
    else:
        await q.edit_message_text("Рассылка отменена.")

async def test_parse(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ У вас нет прав на эту команду.")
        return
    await update.message.reply_text("🔄 Запускаю тестовый парсинг...")
    logger.info(f"Команда /testparse получена от админа {update.effective_user.id}")

    try:
        users = await Database.get_active_subscribers()
        if not users:
            await update.message.reply_text("❌ Нет активных подписчиков. Выдайте себе подписку через /grant")
            return
        await update.message.reply_text(f"✅ Найдено активных подписчиков: {len(users)}")
        for user_id, filters_json in users[:3]:
            filters = json.loads(filters_json)
            districts = filters.get('districts', [])
            rooms = filters.get('rooms', [])
            metros = filters.get('metros', [])
            owner_only = filters.get('owner_only', False)
            await update.message.reply_text(f"👤 Пользователь {user_id}: округов {len(districts)}, комнат {len(rooms)}, станций {len(metros)}, собственники только: {owner_only}")

            ads = await fetch_cian(districts, rooms, metros, owner_only)
            if ads is None:
                await update.message.reply_text(f"❌ fetch_cian вернул None")
            elif len(ads) == 0:
                await update.message.reply_text(f"ℹ️ Объявлений не найдено")
            else:
                await update.message.reply_text(f"✅ Найдено объявлений: {len(ads)}")
                if ads:
                    ad = ads[0]
                    sample = f"🔹 {ad['title']}\n💰 {ad['price']}\n📍 {ad['address']}\n🚇 {ad['metro']}\n🛏 {ad['rooms']}\n👤 {'Собственник' if ad['owner'] else 'Агент'}"
                    await update.message.reply_text(sample[:500])
        await update.message.reply_text("✅ Тест завершён. Проверьте логи.")
    except Exception as e:
        error_msg = f"❌ Ошибка: {type(e).__name__}: {e}"
        await update.message.reply_text(error_msg)
        logger.exception("Ошибка в test_parse")

# ========== ЗАПУСК ==========
async def post_init(app: Application):
    asyncio.create_task(background_parser(app))

def main():
    app = Application.builder().token(TOKEN).post_init(post_init).build()

    # Основные команды
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('status', my_status))
    app.add_handler(CallbackQueryHandler(my_status, pattern='^st$'))

    # Подписка
    app.add_handler(CallbackQueryHandler(choose_plan, pattern='^cp$'))
    app.add_handler(CallbackQueryHandler(plan_chosen, pattern='^p\\d+m$'))
    app.add_handler(CallbackQueryHandler(back_to_start, pattern='^bk$'))

    # Фильтры – основное меню
    app.add_handler(CallbackQueryHandler(start_filter, pattern='^fl$'))
    app.add_handler(CallbackQueryHandler(filter_districts, pattern='^f_districts$'))
    app.add_handler(CallbackQueryHandler(filter_rooms, pattern='^f_rooms$'))
    app.add_handler(CallbackQueryHandler(filter_metros, pattern='^f_metros$'))
    app.add_handler(CallbackQueryHandler(filter_owner, pattern='^f_owner$'))
    app.add_handler(CallbackQueryHandler(filters_done, pattern='^f_done$'))
    app.add_handler(CallbackQueryHandler(filter_back, pattern='^f_back$'))

    # Выбор округов
    app.add_handler(CallbackQueryHandler(toggle_district, pattern='^d_.+$'))

    # Выбор комнат
    app.add_handler(CallbackQueryHandler(toggle_room, pattern='^r_.+$'))

    # Выбор метро
    app.add_handler(CallbackQueryHandler(metro_line, pattern='^l_.+$'))
    app.add_handler(CallbackQueryHandler(toggle_metro, pattern='^m_.+$'))

    # Выбор типа
    app.add_handler(CallbackQueryHandler(toggle_owner, pattern='^owner_'))

    # Платёжные подтверждения
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
    app.add_handler(CommandHandler('daily', daily_by_metro))
    app.add_handler(CallbackQueryHandler(broadcast_confirm, pattern='^bc_'))

    logger.info("Бот запускается...")
    app.run_polling()

if __name__ == '__main__':
    asyncio.run(Database.init())
    main()