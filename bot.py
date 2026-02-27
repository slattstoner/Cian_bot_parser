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
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    filters,
    ContextTypes
)
from telegram.constants import ParseMode

# Selenium и undetected-chromedriver
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains

# ========== НАСТРОЙКИ ==========
TOKEN = os.environ.get('TOKEN')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 0))
TON_WALLET = os.environ.get('TON_WALLET', '')
DADATA_API_KEY = os.environ.get('DADATA_API_KEY', '')
DATABASE_URL = os.environ.get('DATABASE_URL')
PROXY_URL = os.environ.get('PROXY_URL', None)

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
            # Таблица для тикетов поддержки
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS support_tickets (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    message TEXT,
                    created_at BIGINT,
                    status TEXT DEFAULT 'open' -- open, closed
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
            # Количество открытых тикетов
            open_tickets = await conn.fetchval('SELECT COUNT(*) FROM support_tickets WHERE status = $1', 'open')
            return total, active, pending, total_income, monthly, open_tickets

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

# ========== ГЛОБАЛЬНЫЙ ДРАЙВЕР ==========
driver = None
driver_lock = asyncio.Lock()
request_counter = 0
MAX_REQUESTS_PER_DRIVER = 50

async def init_driver():
    global driver
    options = uc.ChromeOptions()
    options.add_argument('--headless=new')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_argument('--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36')
    
    if PROXY_URL:
        options.add_argument(f'--proxy-server={PROXY_URL}')
    
    try:
        driver = uc.Chrome(options=options, version_main=145)
        logger.info("✅ undetected_chromedriver успешно запущен")
    except Exception as e:
        logger.error(f"❌ Ошибка запуска драйвера: {e}")
        raise

async def restart_driver():
    global driver
    async with driver_lock:
        if driver:
            try:
                driver.quit()
            except:
                pass
        await init_driver()

async def get_page_html(url, params=None):
    global driver, request_counter
    async with driver_lock:
        if driver is None:
            await init_driver()
        
        request_counter += 1
        if request_counter >= MAX_REQUESTS_PER_DRIVER:
            logger.info("Перезапуск драйвера по лимиту запросов")
            await restart_driver()
            request_counter = 0
        
        try:
            full_url = url + '?' + urlencode(params) if params else url
            logger.info(f"Загрузка страницы: {full_url}")
            
            driver.get(full_url)
            
            # Имитация человеческого поведения
            time.sleep(random.uniform(2, 5))
            scroll_height = driver.execute_script("return document.body.scrollHeight")
            steps = random.randint(3, 6)
            for i in range(1, steps+1):
                scroll_to = (scroll_height // steps) * i
                driver.execute_script(f"window.scrollTo(0, {scroll_to});")
                time.sleep(random.uniform(0.5, 1.5))
            
            action = ActionChains(driver)
            action.move_by_offset(random.randint(10, 100), random.randint(10, 100)).perform()
            time.sleep(random.uniform(0.5, 1))
            
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "article"))
            )
            
            html = driver.page_source
            logger.info(f"Страница загружена, длина HTML: {len(html)}")
            return html
        except Exception as e:
            logger.error(f"Ошибка при загрузке страницы: {e}")
            await restart_driver()
            return None

# ========== КЭШ ПАРСИНГА ==========
parse_cache = {}
def cache_key(districts, rooms, metros, owner_only):
    return (tuple(sorted(districts)), tuple(sorted(rooms)), tuple(sorted(metros)), owner_only)

async def fetch_cian(districts, rooms, metros, owner_only):
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
        'sort': 'creation_date_desc',
        'p': '1'
    }
    if owner_only:
        params['owner'] = '1'
    
    for d in districts:
        code = {'ЦАО':8, 'САО':9, 'СВАО':10, 'ВАО':11, 'ЮВАО':12, 'ЮАО':13, 'ЮЗАО':14, 'ЗАО':15, 'СЗАО':16}.get(d)
        if code:
            params[f'okrug[{code}]'] = '1'

    url = "https://www.cian.ru/cat.php"
    html = await get_page_html(url, params)
    if not html:
        logger.error("Не удалось получить HTML страницы")
        return []

    soup = BeautifulSoup(html, 'lxml')
    cards = []
    selectors = [
        ('article', {'data-name': 'CardComponent'}),
        ('div', {'class': '_93444fe79c--card--'}),
        ('div', {'data-testid': 'offer-card'}),
        ('article', {'class': 'offer-card'}),
        ('div', {'class': 'catalog-offers'})
    ]
    for tag, attrs in selectors:
        found = soup.find_all(tag, attrs)
        if found:
            logger.info(f"Найдено карточек по селектору {tag}:{attrs} - {len(found)}")
            cards = found
            break
    else:
        logger.warning("Карточки не найдены ни по одному селектору")
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

            owner_tag = card.find('span', text=re.compile('собственник', re.I))
            is_owner = bool(owner_tag)
            if owner_only and not is_owner:
                continue

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

    parse_cache[key] = (results, now + 300)
    logger.info(f"Успешно распарсено {len(results)} объявлений")
    return results

async def fetch_daily_by_metro(metro_stations=None):
    # аналогично предыдущей версии (можно не менять)
    # (код остаётся таким же, для краткости опущен, но в реальном файле он должен быть)
    pass

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

# ========== ФОНОВЫЙ ПАРСИНГ ==========
async def background_parser(app: Application):
    while True:
        try:
            users = await Database.get_active_subscribers()
            for user_id, filters_json in users:
                filters = json.loads(filters_json)
                districts = filters.get('districts', [])
                rooms = filters.get('rooms', [])
                metros = filters.get('metros', [])
                owner_only = filters.get('owner_only', False)
                ads = await fetch_cian(districts, rooms, metros, owner_only)
                if not ads:
                    continue

                user_data = await Database.get_user(user_id)
                last_ad_id = user_data[2] if user_data else None
                new_ads = [a for a in ads if a['id'] != last_ad_id]

                for ad in new_ads[:3]:
                    district_ok = True
                    if districts and ad.get('district_detected'):
                        district_ok = ad['district_detected'] in districts
                    metro_ok = True
                    if metros and ad['metro'] != 'Не указано':
                        metro_ok = ad['metro'] in metros
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

                    owner_ok = True
                    if owner_only:
                        owner_ok = ad['owner']

                    if (not districts and not metros and not rooms) or (district_ok and metro_ok and room_ok and owner_ok):
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
                            if ad['photos']:
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
                                await app.bot.send_media_group(chat_id=user_id, media=media)
                            else:
                                await app.bot.send_message(
                                    chat_id=user_id,
                                    text=text,
                                    parse_mode='Markdown',
                                    disable_web_page_preview=True
                                )
                            await Database.update_last_ad(user_id, ad['id'])
                        except Exception as e:
                            logger.error(f"Ошибка отправки {user_id}: {e}")
        except Exception as e:
            logger.error(f"Ошибка в фоновом парсинге: {e}")
        await asyncio.sleep(600)

# ========== ОБРАБОТЧИКИ КОМАНД И КНОПОК ==========

# Главное меню (кнопки)
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправляет или редактирует главное меню."""
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='cp')],
        [InlineKeyboardButton("ℹ️ Мой профиль", callback_data='profile')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='fl')],
        [InlineKeyboardButton("🆘 Поддержка", callback_data='support')],
        [InlineKeyboardButton("❓ Помощь", callback_data='help')]
    ]
    text = (
        "👋 *Главное меню*\n\n"
        "Выберите действие:"
    )
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
    """Показывает профиль текущего пользователя."""
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
            filters_text = (f"🏘 Округа: {districts}\n🛏 Комнат: {rooms}\n🚇 Метро: {metros}\n👤 Тип: {owner_type}")
        except:
            filters_text = "⚠️ Ошибка в фильтрах"
    else:
        filters_text = "⚙️ Фильтры не настроены"

    # Информация о пользователе Telegram
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

# ---------- ПОДДЕРЖКА ----------
async def support_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начало обращения в поддержку."""
    text = "🆘 Напишите ваш вопрос или проблему. Мы ответим вам в ближайшее время."
    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]]
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    # Устанавливаем состояние, что пользователь ожидает ввода сообщения для поддержки
    context.user_data['awaiting_support'] = True

async def handle_support_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обрабатывает сообщение от пользователя для поддержки."""
    if not context.user_data.get('awaiting_support'):
        return  # не в режиме поддержки
    user_id = update.effective_user.id
    message_text = update.message.text
    # Сохраняем тикет в БД
    ticket_id = await Database.create_ticket(user_id, message_text)
    # Пересылаем админу
    user = update.effective_user
    forward_text = (
        f"🆘 *Новое обращение в поддержку*\n"
        f"От: {user.full_name} (@{user.username})\n"
        f"ID: `{user_id}`\n"
        f"Тикет #{ticket_id}\n\n"
        f"*Сообщение:*\n{message_text}"
    )
    await context.bot.send_message(chat_id=ADMIN_ID, text=forward_text, parse_mode='Markdown')
    # Подтверждение пользователю
    await update.message.reply_text("✅ Ваше сообщение отправлено администратору. Ожидайте ответа.")
    # Выходим из режима поддержки
    context.user_data['awaiting_support'] = False
    # Показываем главное меню
    await main_menu(update, context)

# Админ может ответить на тикет (переслать сообщение пользователю)
async def admin_reply_to_ticket(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Админ отправляет ответ пользователю. Использование: /reply <user_id> <текст>"""
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        parts = update.message.text.split(maxsplit=2)
        if len(parts) < 3:
            await update.message.reply_text("Использование: /reply user_id текст")
            return
        user_id = int(parts[1])
        reply_text = parts[2]
        await context.bot.send_message(chat_id=user_id, text=f"📬 *Ответ администратора:*\n{reply_text}", parse_mode='Markdown')
        await update.message.reply_text(f"✅ Ответ отправлен пользователю {user_id}")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def tickets_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр открытых тикетов (только для админа)."""
    if update.effective_user.id != ADMIN_ID:
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
    """Закрыть тикет (только для админа). /close_ticket <id>"""
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        ticket_id = int(context.args[0])
        await Database.close_ticket(ticket_id)
        await update.message.reply_text(f"Тикет #{ticket_id} закрыт.")
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /close_ticket id")

# ---------- АДМИНСКИЕ КОМАНДЫ (расширенные) ----------
async def profile_by_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Просмотр профиля любого пользователя по ID или username (только админ)."""
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /profile <id или @username>")
        return
    identifier = context.args[0]
    user_id = None
    # Если передан username
    if identifier.startswith('@'):
        # username поиск по базе? В нашей БД нет username, только ID.
        # Можно попросить передать ID. Для упрощения оставим только ID.
        await update.message.reply_text("Пожалуйста, используйте числовой ID. Username не хранятся в БД.")
        return
    try:
        user_id = int(identifier)
    except ValueError:
        await update.message.reply_text("ID должен быть числом.")
        return

    user = await Database.get_user(user_id)
    if not user:
        await update.message.reply_text(f"Пользователь {user_id} не найден.")
        return
    filters, subscribed_until, last_ad_id, plan = user
    now = int(time.time())
    if subscribed_until and subscribed_until > now:
        remaining = (subscribed_until - now) // 86400
        status = f"✅ активна (осталось {remaining} дн.)"
    else:
        status = "❌ не активна"
    f_text = json.loads(filters) if filters else "не настроены"
    text = (
        f"**Профиль пользователя {user_id}**\n"
        f"Статус подписки: {status}\n"
        f"План: {plan if plan else 'не указан'}\n"
        f"Фильтры: {f_text}\n"
        f"Последнее объявление: {last_ad_id or 'нет'}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')

# Остальные админские команды (grant, activate, stats, users, broadcast, testparse, daily) остаются как в предыдущей версии
# Для краткости они не переписаны, но в реальном коде должны быть. Я предполагаю, что вы их оставите.

# ========== ЗАПУСК ==========
async def post_init(app: Application):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: asyncio.run(init_driver()))
    asyncio.create_task(background_parser(app))

def main():
    app = Application.builder().token(TOKEN).post_init(post_init).build()

    # Основные команды
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('menu', main_menu))
    app.add_handler(CallbackQueryHandler(main_menu, pattern='^main_menu$'))
    app.add_handler(CallbackQueryHandler(profile, pattern='^profile$'))
    app.add_handler(CallbackQueryHandler(help_command, pattern='^help$'))
    app.add_handler(CallbackQueryHandler(support_start, pattern='^support$'))

    # Подписка (те же обработчики, что и раньше)
    app.add_handler(CallbackQueryHandler(choose_plan, pattern='^cp$'))
    app.add_handler(CallbackQueryHandler(plan_chosen, pattern='^p\\d+m$'))
    app.add_handler(CallbackQueryHandler(back_to_start, pattern='^bk$'))

    # Фильтры (те же обработчики)
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

    # Поддержка
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_support_message))

    # Админские команды
    app.add_handler(CommandHandler('act', activate))  # нужно определить
    app.add_handler(CommandHandler('grant', grant))
    app.add_handler(CommandHandler('stats', stats))
    app.add_handler(CommandHandler('users', users_list))
    app.add_handler(CommandHandler('find', find_user))  # старая команда find
    app.add_handler(CommandHandler('profile', profile_by_id))
    app.add_handler(CommandHandler('tickets', tickets_list))
    app.add_handler(CommandHandler('close_ticket', close_ticket))
    app.add_handler(CommandHandler('reply', admin_reply_to_ticket))
    app.add_handler(CommandHandler('broadcast', broadcast))
    app.add_handler(CommandHandler('testparse', test_parse))
    app.add_handler(CommandHandler('daily', daily_by_metro))
    app.add_handler(CallbackQueryHandler(broadcast_confirm, pattern='^bc_'))

    # Платёжные подтверждения
    app.add_handler(MessageHandler(filters.PHOTO, handle_payment_proof))
    # (другие обработчики платежей)

    logger.info("Бот запускается...")
    app.run_polling()

if __name__ == '__main__':
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(Database.init())
    main()