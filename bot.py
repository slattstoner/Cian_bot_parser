# Версия: 3.0.0 (2026-03-01)
# - Полный список линий метро Москвы (включая МЦК, МЦД)
# - Система модераторов с правами
# - Кнопка возврата в главное меню во всех разделах
# - Хранение источника подписки (grant, payment)
# - Все админские функции работают

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
from asyncio import Semaphore

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

from playwright.async_api import async_playwright

# ========== НАСТРОЙКИ ==========
TOKEN = os.environ.get('TOKEN')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 0))
TON_WALLET = os.environ.get('TON_WALLET', '')
DADATA_API_KEY = os.environ.get('DADATA_API_KEY', '')
DATABASE_URL = os.environ.get('DATABASE_URL')
PROXY_URL = os.environ.get('PROXY_URL', None)
PAYMENT_PROVIDER_TOKEN = os.environ.get('PAYMENT_PROVIDER_TOKEN', None)

if not TOKEN or not ADMIN_ID:
    raise ValueError("Задайте TOKEN и ADMIN_ID")
if not DATABASE_URL:
    raise ValueError("Задайте DATABASE_URL")

# Цены подписок
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
DEAL_TYPES = ['sale', 'rent']
DEAL_TYPE_NAMES = {'sale': '🏠 Продажа', 'rent': '🔑 Аренда'}

# ПОЛНЫЙ СПИСОК ЛИНИЙ МЕТРО МОСКВЫ (2026)
METRO_LINES = {
    'sokolnicheskaya': {'name': '🚇 Сокольническая линия', 'stations': [
        "Бульвар Рокоссовского", "Черкизовская", "Преображенская площадь", "Сокольники",
        "Красносельская", "Комсомольская", "Красные ворота", "Чистые пруды", "Лубянка",
        "Охотный ряд", "Библиотека им. Ленина", "Кропоткинская", "Парк культуры",
        "Фрунзенская", "Спортивная", "Воробьёвы горы", "Университет",
        "Проспект Вернадского", "Юго-Западная", "Тропарёво", "Румянцево", "Саларьево",
        "Филатов Луг", "Прокшино", "Ольховая", "Новомосковская", "Потапово"
    ]},
    'zamoskvoretskaya': {'name': '🚇 Замоскворецкая линия', 'stations': [
        "Ховрино", "Беломорская", "Речной вокзал", "Водный стадион", "Войковская",
        "Сокол", "Аэропорт", "Динамо", "Белорусская", "Маяковская", "Тверская",
        "Театральная", "Новокузнецкая", "Павелецкая", "Автозаводская", "Технопарк",
        "Коломенская", "Каширская", "Кантемировская", "Царицыно", "Орехово",
        "Домодедовская", "Красногвардейская", "Алма-Атинская"
    ]},
    'arbatsko_pokrovskaya': {'name': '🚇 Арбатско-Покровская линия', 'stations': [
        "Пятницкое шоссе", "Митино", "Волоколамская", "Мякинино", "Строгино",
        "Крылатское", "Молодёжная", "Кунцевская", "Славянский бульвар", "Парк Победы",
        "Киевская", "Смоленская", "Арбатская", "Площадь Революции", "Курская",
        "Бауманская", "Электрозаводская", "Семёновская", "Партизанская", "Измайловская",
        "Первомайская", "Щёлковская"
    ]},
    'filevskaya': {'name': '🚇 Филёвская линия', 'stations': [
        "Александровский сад", "Арбатская", "Смоленская", "Киевская", "Студенческая",
        "Кутузовская", "Фили", "Багратионовская", "Филевский парк", "Пионерская",
        "Кунцевская"
    ]},
    'koltsevaya': {'name': '🚇 Кольцевая линия', 'stations': [
        "Киевская", "Краснопресненская", "Белорусская", "Новослободская",
        "Проспект Мира", "Комсомольская", "Курская", "Таганская", "Павелецкая",
        "Добрынинская", "Октябрьская", "Парк культуры"
    ]},
    'kaluzhsko_rizhskaya': {'name': '🚇 Калужско-Рижская линия', 'stations': [
        "Медведково", "Бабушкинская", "Свиблово", "Ботанический сад", "ВДНХ",
        "Алексеевская", "Рижская", "Проспект Мира", "Сухаревская", "Тургеневская",
        "Китай-город", "Третьяковская", "Октябрьская", "Шаболовская", "Ленинский проспект",
        "Академическая", "Профсоюзная", "Новые Черёмушки", "Калужская", "Беляево",
        "Коньково", "Тёплый Стан", "Ясенево", "Новоясеневская"
    ]},
    'tagansko_krasnopresnenskaya': {'name': '🚇 Таганско-Краснопресненская линия', 'stations': [
        "Планерная", "Сходненская", "Тушинская", "Щукинская", "Октябрьское поле",
        "Полежаевская", "Беговая", "Улица 1905 года", "Баррикадная", "Пушкинская",
        "Кузнецкий мост", "Китай-город", "Таганская", "Пролетарская", "Волгоградский проспект",
        "Текстильщики", "Кузьминки", "Рязанский проспект", "Выхино", "Лермонтовский проспект",
        "Жулебино", "Котельники"
    ]},
    'kalininskaya': {'name': '🚇 Калининская линия', 'stations': [
        "Новокосино", "Новогиреево", "Перово", "Шоссе Энтузиастов", "Авиамоторная",
        "Площадь Ильича", "Марксистская", "Третьяковская"
    ]},
    'solntsevskaya': {'name': '🚇 Солнцевская линия', 'stations': [
        "Деловой центр", "Парк Победы", "Минская", "Ломоносовский проспект",
        "Раменки", "Мичуринский проспект", "Озёрная", "Говорово", "Солнцево",
        "Боровское шоссе", "Новопеределкино", "Рассказовка", "Пыхтино", "Аэропорт Внуково"
    ]},
    'serpukhovsko_timiryazevskaya': {'name': '🚇 Серпуховско-Тимирязевская линия', 'stations': [
        "Алтуфьево", "Бибирево", "Отрадное", "Владыкино", "Петровско-Разумовская",
        "Тимирязевская", "Дмитровская", "Савёловская", "Менделеевская", "Цветной бульвар",
        "Чеховская", "Боровицкая", "Полянка", "Серпуховская", "Тульская", "Нагатинская",
        "Нагорная", "Нахимовский проспект", "Севастопольская", "Чертановская",
        "Южная", "Пражская", "Улица Академика Янгеля", "Аннино", "Бульвар Дмитрия Донского"
    ]},
    'lyublinsko_dmitrovskaya': {'name': '🚇 Люблинско-Дмитровская линия', 'stations': [
        "Селигерская", "Верхние Лихоборы", "Окружная", "Петровско-Разумовская",
        "Фонвизинская", "Бутырская", "Марьина Роща", "Достоевская", "Трубная",
        "Сретенский бульвар", "Чкаловская", "Римская", "Крестьянская застава",
        "Дубровка", "Кожуховская", "Печатники", "Волжская", "Люблино", "Братиславская",
        "Марьино", "Борисово", "Шипиловская", "Зябликово"
    ]},
    'bolshaya_koltsevaya': {'name': '🚇 Большая кольцевая линия', 'stations': [
        "Деловой центр", "Шелепиха", "Хорошёво", "Зорге", "Панфиловская", "Стрешнево",
        "Балтийская", "Коптево", "Лихоборы", "Селигерская", "Верхние Лихоборы",
        "Окружная", "Петровско-Разумовская", "Фонвизинская", "Бутырская", "Марьина Роща",
        "Достоевская", "Трубная", "Сретенский бульвар", "Чкаловская", "Римская",
        "Крестьянская застава", "Дубровка", "Кожуховская", "Печатники", "Волжская",
        "Люблино", "Братиславская", "Марьино", "Борисово", "Шипиловская", "Зябликово",
        "Кленовый бульвар", "Нагатинский Затон", "Каширская", "Варшавская", "Каховская",
        "Зюзино", "Воронцовская", "Новаторская", "Проспект Вернадского", "Мичуринский проспект",
        "Аминьевская", "Давыдково", "Можайская", "Терехово", "Кунцевская", "Мневники",
        "Народное Ополчение", "Карамышевская", "Звенигородская"
    ]},
    'butovskaya': {'name': '🚇 Бутовская линия', 'stations': [
        "Битцевский парк", "Лесопарковая", "Улица Старокачаловская", "Улица Скобелевская",
        "Бульвар Адмирала Ушакова", "Улица Горчакова", "Бунинская аллея"
    ]},
    'monorail': {'name': '🚄 Московский монорельс', 'stations': [
        "Тимирязевская", "Улица Милашенкова", "Телецентр", "Улица Академика Королёва",
        "Выставочный центр", "Улица Сергея Эйзенштейна"
    ]},
    'mck': {'name': '🚈 Московское центральное кольцо', 'stations': [
        "Окружная", "Лихоборы", "Коптево", "Балтийская", "Стрешнево", "Панфиловская",
        "Зорге", "Хорошёво", "Шелепиха", "Деловой центр", "Кутузовская", "Лужники",
        "Площадь Гагарина", "Крымская", "Верхние Котлы", "ЗИЛ", "Автозаводская",
        "Дубровка", "Угрешская", "Новохохловская", "Нижегородская", "Андроновка",
        "Шоссе Энтузиастов", "Соколиная Гора", "Измайлово", "Локомотив", "Бульвар Рокоссовского",
        "Белокаменная", "Ростокино", "Ботанический сад", "Владыкино"
    ]},
    'mcd1': {'name': '🚈 МЦД-1 (Одинцово–Лобня)', 'stations': [
        "Одинцово", "Баковка", "Сколково", "Немчиновка", "Сетунь", "Рабочий Посёлок",
        "Кунцевская", "Славянский бульвар", "Фили", "Тестовская", "Белорусская",
        "Савёловская", "Тимирязевская", "Окружная", "Дегунино", "Бескудниково",
        "Лианозово", "Марк", "Новодачная", "Долгопрудная", "Водники", "Хлебниково",
        "Шереметьевская", "Лобня"
    ]},
    'mcd2': {'name': '🚈 МЦД-2 (Нахабино–Подольск)', 'stations': [
        "Нахабино", "Аникеевка", "Опалиха", "Красногорская", "Павшино", "Пенягино",
        "Волоколамская", "Трикотажная", "Тушинская", "Щукинская", "Стрешнево",
        "Красный Балтиец", "Гражданская", "Дмитровская", "Марьина Роща", "Рижская",
        "Площадь трёх вокзалов", "Курская", "Москва-Товарная", "Калитники", "Текстильщики",
        "Люблино", "Депо", "Перерва", "Курьяново", "Москворечье", "Царицыно",
        "Покровское", "Красный Строитель", "Битца", "Бутово", "Щербинка", "Остафьево",
        "Силикатная", "Подольск"
    ]},
    'mcd3': {'name': '🚈 МЦД-3 (Зеленоград–Раменское)', 'stations': [
        "Зеленоград-Крюково", "Малино", "Фирсановка", "Сходня", "Подрезково",
        "Новоподрезково", "Молжаниново", "Химки", "Левобережная", "Ховрино",
        "Грачёвская", "Моссельмаш", "Лихоборы", "Петровско-Разумовская",
        "Останкино", "Рижская", "Митьково", "Электрозаводская", "Сортировочная",
        "Авиамоторная", "Андроновка", "Перово", "Плющево", "Вешняки", "Выхино",
        "Косино", "Ухтомская", "Люберцы", "Панки", "Томилино", "Красково",
        "Малаховка", "Удельная", "Быково", "Ильинская", "Отдых", "Кратово",
        "Есенинская", "Фабричная", "Раменское"
    ]},
    'mcd4': {'name': '🚈 МЦД-4 (Апрелевка–Железнодорожный)', 'stations': [
        "Апрелевка", "Дачная", "Алабино", "Селятино", "Рассудово", "Ожигово",
        "Бекасово-1", "Бекасово-Центральное", "Мачихино", "Крёкшино",
        "Победа", "Лесной Городок", "Толстопальцево", "Кокошкино", "Санино",
        "Переделкино", "Мичуринец", "Внуково", "Лесопарковая", "Мещерская",
        "Очаково", "Аминьевская", "Матвеевская", "Минская", "Поклонная",
        "Кутузовская", "Тестовская", "Москва-Сити", "Серп и Молот",
        "Нижегородская", "Чухлинка", "Кусково", "Новогиреево", "Реутово",
        "Никольское", "Салтыковская", "Кучино", "Ольгино", "Железнодорожная"
    ]}
}

# Плоский список всех станций (для поиска)
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

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ========== БАЗА ДАННЫХ ==========
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
                    plan TEXT,
                    subscription_source TEXT DEFAULT NULL
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
                    status TEXT DEFAULT 'pending',
                    source TEXT DEFAULT 'ton_manual'
                )
            ''')
            # Таблица тикетов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS support_tickets (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    message TEXT,
                    created_at BIGINT,
                    status TEXT DEFAULT 'open',
                    assigned_to BIGINT DEFAULT NULL
                )
            ''')
            # Таблица модераторов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS moderators (
                    user_id BIGINT PRIMARY KEY,
                    permissions TEXT[] DEFAULT '{"view_tickets"}',
                    added_by BIGINT,
                    added_at BIGINT
                )
            ''')
            # Таблица объявлений
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS ads (
                    id SERIAL PRIMARY KEY,
                    ad_id VARCHAR(255) UNIQUE,
                    source VARCHAR(50) DEFAULT 'cian',
                    deal_type VARCHAR(10) DEFAULT 'sale',
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
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_ads_created ON ads(created_at)')
            try:
                await conn.execute('ALTER TABLE users ADD COLUMN subscription_source TEXT DEFAULT NULL')
            except asyncpg.exceptions.DuplicateColumnError:
                pass
        logger.info("База данных инициализирована")

    @classmethod
    async def get_user(cls, user_id):
        async with cls._pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT filters, subscribed_until, last_ad_id, plan, subscription_source FROM users WHERE user_id = $1',
                user_id
            )
            if row:
                return (row['filters'], row['subscribed_until'], row['last_ad_id'], row['plan'], row['subscription_source'])
            return None

    @classmethod
    async def set_user_filters(cls, user_id, filters_dict):
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, filters) VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE SET filters = EXCLUDED.filters
            ''', user_id, json.dumps(filters_dict))

    @classmethod
    async def activate_subscription(cls, user_id, days, plan=None, source='grant'):
        until = int(time.time()) + days * 86400
        async with cls._pool.acquire() as conn:
            if plan:
                await conn.execute(
                    'UPDATE users SET subscribed_until = $1, plan = $2, subscription_source = $3 WHERE user_id = $4',
                    until, plan, source, user_id
                )
            else:
                await conn.execute(
                    'UPDATE users SET subscribed_until = $1, subscription_source = $2 WHERE user_id = $3',
                    until, source, user_id
                )

    @classmethod
    async def update_last_ad(cls, user_id, ad_id):
        async with cls._pool.acquire() as conn:
            await conn.execute(
                'UPDATE users SET last_ad_id = $1 WHERE user_id = $2',
                ad_id, user_id
            )

    @classmethod
    async def add_payment(cls, user_id, amount_ton, plan, source='ton_manual'):
        async with cls._pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO payments (user_id, amount_ton, plan, source) VALUES ($1, $2, $3, $4) RETURNING id',
                user_id, amount_ton, plan, source
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
            active_plans = await conn.fetch('SELECT plan, subscription_source FROM users WHERE subscribed_until > $1 AND plan IS NOT NULL', now)
            monthly = 0.0
            for (plan, source) in active_plans:
                if plan in PRICES_TON and plan in PLAN_DAYS:
                    monthly += PRICES_TON[plan] / PLAN_DAYS[plan] * 30
            open_tickets = await conn.fetchval('SELECT COUNT(*) FROM support_tickets WHERE status = $1', 'open')
            ads_count = await conn.fetchval('SELECT COUNT(*) FROM ads')
            return total, active, pending, total_income, monthly, open_tickets, ads_count

    @classmethod
    async def get_all_users(cls, limit=20, offset=0):
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT user_id, subscribed_until, plan, subscription_source FROM users ORDER BY user_id LIMIT $1 OFFSET $2', limit, offset)

    @classmethod
    async def get_active_subscribers(cls):
        now = int(time.time())
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT user_id, filters FROM users WHERE subscribed_until > $1', now)

    @classmethod
    async def get_active_subscribers_detailed(cls):
        """Для админа: список активных подписчиков с деталями"""
        now = int(time.time())
        async with cls._pool.acquire() as conn:
            rows = await conn.fetch('''
                SELECT user_id, subscribed_until, plan, subscription_source 
                FROM users 
                WHERE subscribed_until > $1 
                ORDER BY subscribed_until DESC
            ''', now)
            return rows

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

    @classmethod
    async def assign_ticket(cls, ticket_id, moderator_id):
        async with cls._pool.acquire() as conn:
            await conn.execute('UPDATE support_tickets SET assigned_to = $1 WHERE id = $2', moderator_id, ticket_id)

    # ========== Методы для модераторов ==========
    @classmethod
    async def add_moderator(cls, user_id, permissions, added_by):
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO moderators (user_id, permissions, added_by, added_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE SET permissions = EXCLUDED.permissions
            ''', user_id, permissions, added_by, int(time.time()))

    @classmethod
    async def remove_moderator(cls, user_id):
        async with cls._pool.acquire() as conn:
            await conn.execute('DELETE FROM moderators WHERE user_id = $1', user_id)

    @classmethod
    async def get_moderators(cls):
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT * FROM moderators ORDER BY added_at')

    @classmethod
    async def is_moderator(cls, user_id):
        async with cls._pool.acquire() as conn:
            row = await conn.fetchrow('SELECT permissions FROM moderators WHERE user_id = $1', user_id)
            return row['permissions'] if row else None

    @classmethod
    async def has_permission(cls, user_id, perm):
        perms = await cls.is_moderator(user_id)
        return perms and perm in perms

    @classmethod
    async def save_ad(cls, ad):
        async with cls._pool.acquire() as conn:
            try:
                result = await conn.execute('''
                    INSERT INTO ads (ad_id, source, deal_type, title, price, address, metro, rooms, floor, area, owner, district, url, photos, published_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                    ON CONFLICT (ad_id) DO NOTHING
                ''', ad['id'], ad.get('source', 'cian'), ad.get('deal_type', 'sale'),
                   ad['title'], ad['price'], ad['address'], ad['metro'], ad['rooms'],
                   ad['floor'], ad['area'], ad['owner'], ad.get('district_detected'),
                   ad['link'], json.dumps(ad.get('photos', [])), datetime.now())
                return 'INSERT 0 1' in result
            except Exception as e:
                logger.error(f"Ошибка сохранения объявления {ad['id']}: {e}")
                return False

    @classmethod
    async def get_new_ads_since(cls, minutes=10):
        since = datetime.now() - timedelta(minutes=minutes)
        async with cls._pool.acquire() as conn:
            rows = await conn.fetch('SELECT * FROM ads WHERE created_at > $1 ORDER BY created_at DESC', since)
            return [dict(r) for r in rows]

# ========== ПАРСИНГ ==========
async def get_page_html_playwright(url, params=None):
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
            await context.set_extra_http_headers({'Proxy': PROXY_URL})
        page = await context.new_page()
        full_url = url + '?' + urlencode(params) if params else url
        logger.info(f"Загрузка страницы: {full_url}")
        await page.goto(full_url, wait_until='domcontentloaded')
        await page.wait_for_timeout(random.randint(2000, 5000))
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight/2)")
        await page.wait_for_timeout(random.randint(1000, 2000))
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await page.wait_for_selector('article[data-name="CardComponent"]', timeout=10000)
        html = await page.content()
        await browser.close()
        return html

async def fetch_cian_deal_type(deal_type='sale'):
    params = {
        'deal_type': deal_type,
        'engine_version': '2',
        'offer_type': 'flat',
        'region': '1',
        'only_flat': '1',
        'sort': 'creation_date_desc',
        'p': '1'
    }
    for d in DISTRICTS:
        code = {'ЦАО':8, 'САО':9, 'СВАО':10, 'ВАО':11, 'ЮВАО':12, 'ЮАО':13, 'ЮЗАО':14, 'ЗАО':15, 'СЗАО':16}.get(d)
        if code:
            params[f'okrug[{code}]'] = '1'
    url = "https://www.cian.ru/cat.php"
    html = await get_page_html_playwright(url, params)
    if not html:
        return []
    soup = BeautifulSoup(html, 'lxml')
    cards = soup.find_all('article', {'data-name': 'CardComponent'})
    if not cards:
        logger.warning(f"Карточки не найдены для {deal_type}")
        return []
    results = []
    seen_ids = set()
    for card in cards[:30]:
        try:
            link_tag = card.find('a', href=True)
            if not link_tag:
                continue
            link = link_tag['href']
            if not link.startswith('http'):
                link = 'https://www.cian.ru' + link
            ad_id = re.search(r'/(\d+)/?$', link)
            ad_id = ad_id.group(1) if ad_id else str(hash(link))
            if ad_id in seen_ids:
                continue
            seen_ids.add(ad_id)

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
                'deal_type': deal_type,
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
    return results

async def fetch_cian_all():
    sale_ads = await fetch_cian_deal_type('sale')
    rent_ads = await fetch_cian_deal_type('rent')
    all_ads = sale_ads + rent_ads
    seen = set()
    unique_ads = []
    for ad in all_ads:
        if ad['id'] not in seen:
            seen.add(ad['id'])
            unique_ads.append(ad)
    return unique_ads

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
        logger.debug(f"Ошибка DaData: {e}")
    return None

# ========== ФИЛЬТРАЦИЯ ==========
def matches_filters(ad, filters):
    districts = filters.get('districts', [])
    rooms = filters.get('rooms', [])
    metros = filters.get('metros', [])
    owner_only = filters.get('owner_only', False)
    deal_type = filters.get('deal_type', 'sale')

    if not districts and not rooms and not metros and not owner_only:
        return False

    if ad.get('deal_type') != deal_type:
        return False

    if districts and ad.get('district_detected'):
        if ad['district_detected'] not in districts:
            return False

    if metros and ad['metro'] != 'Не указано':
        ad_metro_clean = ad['metro'].lower().replace('м.', '').strip()
        found = False
        for m in metros:
            if m.lower() in ad_metro_clean or ad_metro_clean in m.lower():
                found = True
                break
        if not found:
            return False

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

    if owner_only and not ad['owner']:
        return False

    return True

# ========== ФОНОВЫЙ СБОРЩИК ==========
telegram_semaphore = Semaphore(20)

async def collector_loop(app: Application):
    while True:
        try:
            logger.info("Запуск сбора объявлений")
            subscribers = await Database.get_active_subscribers()
            if not subscribers:
                logger.info("Нет активных подписчиков")
                await asyncio.sleep(600)
                continue

            ads = await fetch_cian_all()
            if not ads:
                logger.info("Нет новых объявлений")
                await asyncio.sleep(600)
                continue

            new_ads = []
            for ad in ads:
                if await Database.save_ad(ad):
                    new_ads.append(ad)

            if not new_ads:
                logger.info("Нет новых объявлений после проверки БД")
                await asyncio.sleep(600)
                continue

            logger.info(f"Найдено {len(new_ads)} новых объявлений, начинаем рассылку")

            for ad in new_ads:
                tasks = []
                for user_id, filters_json in subscribers:
                    if not filters_json:
                        continue
                    filters = json.loads(filters_json) if filters_json else {}
                    if not filters.get('districts') and not filters.get('rooms') and not filters.get('metros') and not filters.get('owner_only'):
                        continue
                    if matches_filters(ad, filters):
                        tasks.append(send_ad_to_user(app.bot, user_id, ad))
                if tasks:
                    await asyncio.gather(*tasks)
                    await asyncio.sleep(1)

            logger.info("Рассылка завершена")
        except Exception as e:
            logger.error(f"Ошибка в collector_loop: {e}", exc_info=True)
        await asyncio.sleep(600)

async def send_ad_to_user(bot, user_id, ad):
    async with telegram_semaphore:
        owner_text = "Собственник" if ad['owner'] else "Агент"
        deal_text = "Продажа" if ad.get('deal_type') == 'sale' else "Аренда"
        text = (
            f"🔵 *Новое объявление ({deal_text})*\n"
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
            await Database.update_last_ad(user_id, ad['id'])
            await asyncio.sleep(0.05)
        except Exception as e:
            logger.error(f"Ошибка отправки пользователю {user_id}: {e}")

# ========== ОБРАБОТЧИКИ КОМАНД ==========
async def main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    keyboard = [
        [InlineKeyboardButton("💳 Подписаться", callback_data='cp')],
        [InlineKeyboardButton("ℹ️ Мой профиль", callback_data='profile')],
        [InlineKeyboardButton("⚙️ Настроить фильтры", callback_data='fl')],
        [InlineKeyboardButton("🆘 Поддержка", callback_data='support')],
        [InlineKeyboardButton("❓ Помощь", callback_data='help')]
    ]
    # Если это админ, добавим кнопку админки
    if update.effective_user.id == ADMIN_ID:
        keyboard.append([InlineKeyboardButton("👑 Админ-панель", callback_data='admin_panel')])
    # Если модератор, добавим кнопку модератора
    else:
        perms = await Database.is_moderator(update.effective_user.id)
        if perms:
            keyboard.append([InlineKeyboardButton("🛡 Модератор", callback_data='mod_panel')])

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
        "⚙️ *Настроить фильтры* – выбор округов, комнат, метро, типа объявлений, типа сделки.\n"
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
            owner_type = "Только собственники" if f.get('owner_only') else "Все"
            deal_type = DEAL_TYPE_NAMES.get(f.get('deal_type', 'sale'), 'Продажа')
            filters_text = (f"🏘 Округа: {districts}\n🛏 Комнат: {rooms}\n🚇 Метро: {metros}\n"
                            f"👤 Тип: {owner_type}\n📋 Сделка: {deal_type}")
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

# ---------- ПОДПИСКА ----------
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
    plan = q.data[1:]
    context.user_data['plan'] = plan
    if PAYMENT_PROVIDER_TOKEN:
        await send_invoice(q, context, plan)
    else:
        await pay_ton_manual(q, context)

async def send_invoice(update, context, plan):
    user_id = update.from_user.id
    amount_rub = PRICES_RUB[plan]
    title = f"Подписка на {plan}"
    description = f"Доступ к мониторингу ЦИАН на {PLAN_DAYS[plan]} дней"
    payload = f"sub_{plan}_{user_id}_{int(time.time())}"
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

async def pay_ton_manual(update, context):
    q = update if isinstance(update, Update) else update
    user_id = q.from_user.id
    plan = context.user_data.get('plan', '1m')
    amount = PRICES_TON[plan]
    payment_id = await Database.add_payment(user_id, amount, plan, source='ton_manual')
    text = (
        f"**Оплата в TON**\n\n"
        f"Сумма: **{amount} TON**\n"
        f"Кошелёк: `{TON_WALLET}`\n\n"
        "После перевода **отправьте TXID** (или скриншот).\n"
        "Администратор проверит и активирует подписку.\n\n"
        f"**ID платежа:** `{payment_id}`"
    )
    await q.edit_message_text(text, parse_mode='Markdown')

async def pre_checkout(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.pre_checkout_query
    await query.answer(ok=True)

async def successful_payment(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    payload = update.message.successful_payment.invoice_payload
    parts = payload.split('_')
    if len(parts) >= 2:
        plan = parts[1]
        days = PLAN_DAYS.get(plan, 30)
        await Database.activate_subscription(user_id, days, plan, source='payment_telegram')
        await update.message.reply_text("✅ Оплата прошла успешно! Подписка активирована.")
    else:
        await update.message.reply_text("✅ Оплата прошла, но возникла ошибка с активацией. Обратитесь в поддержку.")

# ---------- ФИЛЬТРЫ ----------
async def start_filter(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    context.user_data['districts'] = []
    context.user_data['rooms'] = []
    context.user_data['metros'] = []
    context.user_data['owner_only'] = False
    context.user_data['deal_type'] = 'sale'
    keyboard = [
        [InlineKeyboardButton("🏘 Выбрать округа", callback_data='f_districts')],
        [InlineKeyboardButton("🛏 Выбрать комнаты", callback_data='f_rooms')],
        [InlineKeyboardButton("🚇 Выбрать метро", callback_data='f_metros')],
        [InlineKeyboardButton("👤 Выбрать тип", callback_data='f_owner')],
        [InlineKeyboardButton("📋 Тип сделки", callback_data='f_deal_type')],
        [InlineKeyboardButton("✅ Завершить настройку", callback_data='f_done')],
        [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
    ]
    await q.edit_message_text("⚙️ **Настройка фильтров**\nВыберите, что хотите настроить:", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

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
    keyboard = []
    for station in found[:10]:
        keyboard.append([InlineKeyboardButton(station, callback_data=f'm_{station}')])
    keyboard.append([InlineKeyboardButton("« Отмена", callback_data='f_metros')])
    await update.message.reply_text("Найдено станций. Выберите:", reply_markup=InlineKeyboardMarkup(keyboard))
    context.user_data['awaiting_metro_search'] = False

async def filter_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    current = context.user_data.get('owner_only', False)
    text = "👤 Выберите тип объявлений:\n"
    keyboard = [
        [InlineKeyboardButton("✅ Все (агенты и собственники)" if not current else "⬜ Все (агенты и собственники)", callback_data='owner_all')],
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
        [InlineKeyboardButton("✅ Все (агенты и собственники)" if not current else "⬜ Все (агенты и собственники)", callback_data='owner_all')],
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
    filters = {
        'city': 'Москва',
        'districts': districts,
        'rooms': rooms,
        'metros': metros,
        'owner_only': owner_only,
        'deal_type': deal_type
    }
    await Database.set_user_filters(user_id, filters)
    deal_name = DEAL_TYPE_NAMES.get(deal_type, 'Продажа')
    text = "✅ **Фильтры сохранены!**\n\n🏙 Город: Москва\n"
    text += f"🏘 Округа: {', '.join(districts) if districts else 'все'}\n"
    text += f"🛏 Комнат: {', '.join(rooms) if rooms else 'все'}\n"
    text += f"🚇 Метро: {', '.join(metros) if metros else 'все'}\n"
    text += f"👤 Тип: {'Только собственники' if owner_only else 'Все'}\n"
    text += f"📋 Сделка: {deal_name}"
    await q.edit_message_text(text, parse_mode='Markdown')
    await main_menu(update, context)

# ---------- ПОДДЕРЖКА ----------
async def support_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = "🆘 Напишите ваш вопрос или проблему. Мы ответим вам в ближайшее время."
    keyboard = [[InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]]
    if update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    else:
        await update.message.reply_text(text, parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))
    context.user_data['awaiting_support'] = True

async def handle_support_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('awaiting_support'):
        return
    user_id = update.effective_user.id
    message_text = update.message.text
    ticket_id = await Database.create_ticket(user_id, message_text)
    user = update.effective_user
    forward_text = (
        f"🆘 *Новое обращение в поддержку*\n"
        f"От: {user.full_name} (@{user.username})\n"
        f"ID: `{user_id}`\n"
        f"Тикет #{ticket_id}\n\n"
        f"*Сообщение:*\n{message_text}"
    )
    # Уведомляем админов и модераторов
    await context.bot.send_message(chat_id=ADMIN_ID, text=forward_text, parse_mode='Markdown')
    mods = await Database.get_moderators()
    for mod in mods:
        if await Database.has_permission(mod['user_id'], 'view_tickets'):
            try:
                await context.bot.send_message(chat_id=mod['user_id'], text=forward_text, parse_mode='Markdown')
            except:
                pass
    await update.message.reply_text("✅ Ваше сообщение отправлено администратору. Ожидайте ответа.")
    context.user_data['awaiting_support'] = False
    await main_menu(update, context)

# ---------- АДМИНСКИЕ КОМАНДЫ ----------
async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    keyboard = [
        [InlineKeyboardButton("📊 Статистика", callback_data='admin_stats')],
        [InlineKeyboardButton("👥 Список пользователей", callback_data='admin_users_0')],
        [InlineKeyboardButton("🆘 Открытые тикеты", callback_data='admin_tickets')],
        [InlineKeyboardButton("📢 Сделать рассылку", callback_data='admin_broadcast')],
        [InlineKeyboardButton("🔍 Поиск пользователя", callback_data='admin_find')],
        [InlineKeyboardButton("👥 Активные подписчики", callback_data='admin_active_subs')],
        [InlineKeyboardButton("➕ Добавить модератора", callback_data='admin_add_mod')],
        [InlineKeyboardButton("➖ Удалить модератора", callback_data='admin_remove_mod')],
        [InlineKeyboardButton("📋 Список модераторов", callback_data='admin_list_mods')],
        [InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')]
    ]
    await update.message.reply_text("🔧 *Админ-панель*", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.callback_query.answer()
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
    await update.callback_query.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await update.callback_query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

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
    text = f"**Список пользователей (страница {offset//20 + 1}):**\n"
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
        keyboard.append(InlineKeyboardButton("⬅️ Назад", callback_data=f'admin_users_{offset-20}'))
    if len(rows) == 20:
        keyboard.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f'admin_users_{offset+20}'))
    keyboard.append([InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')])
    reply_markup = InlineKeyboardMarkup(keyboard) if keyboard else None
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def admin_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.callback_query.answer()
    tickets = await Database.get_open_tickets()
    if not tickets:
        text = "Нет открытых тикетов."
    else:
        text = "🆘 *Открытые тикеты:*\n\n"
        for t in tickets:
            text += f"#{t['id']} от `{t['user_id']}`: {t['message'][:50]}...\n"
    await update.callback_query.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await update.callback_query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_broadcast_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        "Используйте команду /broadcast <текст> для рассылки.\n\nНапример: /broadcast Всем привет!",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]])
    )

async def admin_find_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.callback_query.answer()
    await update.callback_query.edit_message_text(
        "Используйте команду /find <user_id> для поиска пользователя.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]])
    )

async def admin_active_subs_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.callback_query.answer()
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
    await update.callback_query.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await update.callback_query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_add_mod_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.callback_query.answer()
    context.user_data['awaiting_mod_user_id'] = True
    await update.callback_query.edit_message_text(
        "Введите ID пользователя, которого хотите сделать модератором:",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Отмена", callback_data='admin_panel_back')]])
    )

async def admin_handle_add_mod(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get('awaiting_mod_user_id') or update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(update.message.text.strip())
        perms = ['view_tickets', 'view_stats']
        await Database.add_moderator(user_id, perms, ADMIN_ID)
        await update.message.reply_text(f"✅ Пользователь {user_id} добавлен как модератор с правами: {perms}")
        context.user_data['awaiting_mod_user_id'] = False
        await admin_panel(update, context)
    except ValueError:
        await update.message.reply_text("❌ Неверный ID. Введите число.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def admin_remove_mod_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.callback_query.answer()
    mods = await Database.get_moderators()
    if not mods:
        await update.callback_query.edit_message_text("Нет модераторов.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]]))
        return
    keyboard = []
    for m in mods:
        keyboard.append([InlineKeyboardButton(f"Удалить {m['user_id']}", callback_data=f'remove_mod_{m["user_id"]}')])
    keyboard.append([InlineKeyboardButton("« Назад", callback_data='admin_panel_back')])
    await update.callback_query.edit_message_text("Выберите модератора для удаления:", reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_remove_mod_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    q = update.callback_query
    await q.answer()
    user_id = int(q.data.split('_')[2])
    await Database.remove_moderator(user_id)
    await q.edit_message_text(f"✅ Модератор {user_id} удалён.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("« Назад", callback_data='admin_panel_back')]]))

async def admin_list_mods_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await update.callback_query.answer()
    mods = await Database.get_moderators()
    if not mods:
        text = "Нет модераторов."
    else:
        text = "**Список модераторов:**\n\n"
        for m in mods:
            text += f"• `{m['user_id']}` | права: {', '.join(m['permissions'])}\n"
    await update.callback_query.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад в админку", callback_data='admin_panel_back')]]
    await update.callback_query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_panel_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    await admin_panel(update, context)

# ---------- МОДЕРАТОРСКАЯ ПАНЕЛЬ ----------
async def mod_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    perms = await Database.is_moderator(user_id)
    if not perms:
        await update.message.reply_text("⛔ Доступ запрещён.")
        return
    keyboard = []
    if 'view_tickets' in perms:
        keyboard.append([InlineKeyboardButton("🆘 Открытые тикеты", callback_data='mod_tickets')])
    if 'view_stats' in perms:
        keyboard.append([InlineKeyboardButton("📊 Статистика", callback_data='mod_stats')])
    keyboard.append([InlineKeyboardButton("🏠 Главное меню", callback_data='main_menu')])
    await update.message.reply_text("🛡 *Панель модератора*", parse_mode='Markdown', reply_markup=InlineKeyboardMarkup(keyboard))

async def mod_tickets_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await Database.has_permission(user_id, 'view_tickets'):
        return
    await update.callback_query.answer()
    tickets = await Database.get_open_tickets()
    if not tickets:
        text = "Нет открытых тикетов."
    else:
        text = "🆘 *Открытые тикеты:*\n\n"
        for t in tickets:
            text += f"#{t['id']} от `{t['user_id']}`: {t['message'][:50]}...\n"
    await update.callback_query.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад", callback_data='mod_panel_back')]]
    await update.callback_query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def mod_stats_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await Database.has_permission(user_id, 'view_stats'):
        return
    await update.callback_query.answer()
    total, active, pending, total_income, monthly, open_tickets, ads_count = await Database.get_stats()
    text = (
        f"📊 **Статистика для модератора**\n"
        f"👥 Всего пользователей: {total}\n"
        f"✅ Активных подписок: {active}\n"
        f"🆘 Открытых тикетов: {open_tickets}\n"
        f"📰 Объявлений в базе: {ads_count}\n"
        f"💰 Доход скрыт (нет прав)."
    )
    await update.callback_query.edit_message_text(text, parse_mode='Markdown')
    keyboard = [[InlineKeyboardButton("« Назад", callback_data='mod_panel_back')]]
    await update.callback_query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup(keyboard))

async def mod_panel_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    if not await Database.is_moderator(user_id):
        return
    await mod_panel(update, context)

# ---------- ПЛАТЕЖИ (старый метод) ----------
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

# ---------- АДМИНСКИЕ КОМАНДЫ (текстовые) ----------
async def activate(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        plan = await Database.get_pending_plan(user_id)
        if plan:
            days = PLAN_DAYS[plan]
            await Database.activate_subscription(user_id, days, plan, source='payment_ton')
            await Database.confirm_payment(user_id, plan)
            await update.message.reply_text(f"✅ Подписка для {user_id} активирована на {days} дней (источник: TON ручной).")
            await context.bot.send_message(chat_id=user_id, text="✅ Ваша подписка активирована! Настройте фильтры в главном меню.")
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
        await Database.activate_subscription(user_id, days, plan, source='grant')
        await update.message.reply_text(f"✅ Подписка для {user_id} на {days} дней (источник: grant).")
        await context.bot.send_message(chat_id=user_id, text=f"✅ Администратор выдал подписку на {days} дней! Настройте фильтры.")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}. Использование: /grant user_id days [plan]")

async def active_subs_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def users_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        offset = int(context.args[0]) if context.args else 0
    except:
        offset = 0
    rows = await Database.get_all_users(limit=20, offset=offset)
    if not rows:
        await update.message.reply_text("Нет пользователей.")
        return
    text = f"**Список пользователей (страница {offset//20 + 1}):**\n"
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
        keyboard.append(InlineKeyboardButton("⬅️ Назад", callback_data=f'users_page_{offset-20}'))
    if len(rows) == 20:
        keyboard.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f'users_page_{offset+20}'))
    reply_markup = InlineKeyboardMarkup([keyboard]) if keyboard else None
    await update.message.reply_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def users_page(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    offset = int(q.data.split('_')[2])
    rows = await Database.get_all_users(limit=20, offset=offset)
    if not rows:
        await q.edit_message_text("Нет пользователей.")
        return
    text = f"**Список пользователей (страница {offset//20 + 1}):**\n"
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
        keyboard.append(InlineKeyboardButton("⬅️ Назад", callback_data=f'users_page_{offset-20}'))
    if len(rows) == 20:
        keyboard.append(InlineKeyboardButton("Вперёд ➡️", callback_data=f'users_page_{offset+20}'))
    reply_markup = InlineKeyboardMarkup([keyboard]) if keyboard else None
    await q.edit_message_text(text, parse_mode='Markdown', reply_markup=reply_markup)

async def find_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    try:
        user_id = int(context.args[0])
        user = await Database.get_user(user_id)
        if not user:
            await update.message.reply_text("Пользователь не найден.")
            return
        filters, until, last_ad, plan, source = user
        now = int(time.time())
        status = f"✅ активна (осталось {(until-now)//86400} дн.)" if until and until > now else "❌ не активна"
        f_text = json.loads(filters) if filters else "не настроены"
        text = f"**Пользователь {user_id}**\nСтатус: {status}\nПлан: {plan}\nИсточник: {source}\nФильтры: {f_text}\nПоследнее объявление: {last_ad}"
        await update.message.reply_text(text, parse_mode='Markdown')
    except (IndexError, ValueError):
        await update.message.reply_text("Использование: /find user_id")
    except Exception as e:
        await update.message.reply_text(f"Ошибка: {e}")

async def profile_by_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        return
    if not context.args:
        await update.message.reply_text("Использование: /profile <id>")
        return
    try:
        user_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text("ID должен быть числом.")
        return
    user = await Database.get_user(user_id)
    if not user:
        await update.message.reply_text(f"Пользователь {user_id} не найден.")
        return
    filters, subscribed_until, last_ad_id, plan, source = user
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
        f"Источник: {source}\n"
        f"Фильтры: {f_text}\n"
        f"Последнее объявление: {last_ad_id or 'нет'}"
    )
    await update.message.reply_text(text, parse_mode='Markdown')

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
        for (user_id, _, _, _) in rows:
            try:
                await context.bot.send_message(chat_id=user_id, text=text, parse_mode='Markdown')
                success += 1
                await asyncio.sleep(0.05)
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
    try:
        ads = await fetch_cian_all()
        if not ads:
            await update.message.reply_text("❌ Объявлений не найдено.")
        else:
            await update.message.reply_text(f"✅ Найдено объявлений: {len(ads)}. Первое: {ads[0]['title']}")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

async def daily_by_metro(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("⛔ Эта команда только для администратора.")
        return
    args = context.args
    if not args:
        await update.message.reply_text("ℹ️ Использование: /daily станция1 станция2 ...")
        return
    stations_text = ' '.join(args)
    stations = [s.strip() for s in stations_text.split() if s.strip()]
    await update.message.reply_text(f"🔄 Ищу свежие объявления по станциям: {', '.join(stations)}...")
    try:
        ads = await fetch_cian_all()
        if not ads:
            await update.message.reply_text("❌ Объявлений не найдено.")
            return
        filtered = [ad for ad in ads if any(st.lower() in ad['metro'].lower() for st in stations)]
        if not filtered:
            await update.message.reply_text("❌ Нет объявлений по указанным станциям.")
            return
        await update.message.reply_text(f"✅ Найдено объявлений: {len(filtered)}")
        for ad in filtered[:5]:
            owner = "Собственник" if ad['owner'] else "Агент"
            text = (
                f"🔵 *{ad['title']}*\n"
                f"💰 Цена: {ad['price']}\n📍 Адрес: {ad['address']}\n"
                f"🚇 Метро: {ad['metro']}\n🏢 Этаж: {ad['floor']}\n"
                f"📏 Площадь: {ad['area']}\n🛏 Комнат: {ad['rooms']}\n"
                f"👤 {owner}\n[Ссылка]({ad['link']})"
            )
            await update.message.reply_text(text, parse_mode='Markdown', disable_web_page_preview=True)
        if len(filtered) > 5:
            await update.message.reply_text(f"... и ещё {len(filtered)-5} объявлений.")
    except Exception as e:
        await update.message.reply_text(f"❌ Ошибка: {e}")

# ========== ЗАПУСК ==========
async def post_init(app: Application):
    await Database.init()
    asyncio.create_task(collector_loop(app))

def main():
    app = Application.builder().token(TOKEN).post_init(post_init).build()

    # Основные команды
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CommandHandler('menu', main_menu))
    app.add_handler(CommandHandler('admin', admin_panel))
    app.add_handler(CommandHandler('mod', mod_panel))
    app.add_handler(CommandHandler('active_subs', active_subs_command))

    # Обработчики колбэков
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
    app.add_handler(CallbackQueryHandler(filter_deal_type, pattern='^f_deal_type$'))
    app.add_handler(CallbackQueryHandler(filters_done, pattern='^f_done$'))
    app.add_handler(CallbackQueryHandler(filter_back, pattern='^f_back$'))
    app.add_handler(CallbackQueryHandler(toggle_district, pattern='^d_.+$'))
    app.add_handler(CallbackQueryHandler(toggle_room, pattern='^r_.+$'))
    app.add_handler(CallbackQueryHandler(metro_line, pattern='^l_.+$'))
    app.add_handler(CallbackQueryHandler(toggle_metro, pattern='^m_.+$'))
    app.add_handler(CallbackQueryHandler(toggle_owner, pattern='^owner_'))
    app.add_handler(CallbackQueryHandler(toggle_deal_type, pattern='^deal_'))
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

    # Админские колбэки
    app.add_handler(CallbackQueryHandler(admin_panel_back, pattern='^admin_panel_back$'))
    app.add_handler(CallbackQueryHandler(admin_stats_callback, pattern='^admin_stats$'))
    app.add_handler(CallbackQueryHandler(admin_users_callback, pattern='^admin_users_'))
    app.add_handler(CallbackQueryHandler(admin_tickets_callback, pattern='^admin_tickets$'))
    app.add_handler(CallbackQueryHandler(admin_broadcast_callback, pattern='^admin_broadcast$'))
    app.add_handler(CallbackQueryHandler(admin_find_callback, pattern='^admin_find$'))
    app.add_handler(CallbackQueryHandler(admin_active_subs_callback, pattern='^admin_active_subs$'))
    app.add_handler(CallbackQueryHandler(admin_add_mod_callback, pattern='^admin_add_mod$'))
    app.add_handler(CallbackQueryHandler(admin_remove_mod_callback, pattern='^admin_remove_mod$'))
    app.add_handler(CallbackQueryHandler(admin_remove_mod_confirm, pattern='^remove_mod_'))
    app.add_handler(CallbackQueryHandler(admin_list_mods_callback, pattern='^admin_list_mods$'))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, admin_handle_add_mod))

    # Модераторские колбэки
    app.add_handler(CallbackQueryHandler(mod_panel_back, pattern='^mod_panel_back$'))
    app.add_handler(CallbackQueryHandler(mod_tickets_callback, pattern='^mod_tickets$'))
    app.add_handler(CallbackQueryHandler(mod_stats_callback, pattern='^mod_stats$'))

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