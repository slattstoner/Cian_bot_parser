import asyncpg
import json
import time
import logging
from typing import Optional, List, Tuple, Any
from datetime import datetime, timedelta
from models import Ad

logger = logging.getLogger(__name__)


class Database:
    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def init(cls, dsn: str):
        cls._pool = await asyncpg.create_pool(dsn, min_size=5, max_size=20)
        async with cls._pool.acquire() as conn:
            # Таблица пользователей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    user_id BIGINT PRIMARY KEY,
                    role TEXT DEFAULT 'user',
                    referrer_id BIGINT DEFAULT NULL,
                    filters TEXT,
                    subscribed_until BIGINT,
                    last_ad_id TEXT,
                    plan TEXT,
                    subscription_source TEXT DEFAULT NULL,
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
                    banned BOOLEAN DEFAULT FALSE
                )
            ''')
            # Добавляем колонки если их нет (для существующих БД)
            for col, definition in [
                ('banned', 'BOOLEAN DEFAULT FALSE'),
                ('subscription_source', 'TEXT DEFAULT NULL'),
                ('role', "TEXT DEFAULT 'user'"),
            ]:
                try:
                    await conn.execute(f'ALTER TABLE users ADD COLUMN IF NOT EXISTS {col} {definition}')
                except Exception:
                    pass

            # Таблица рефералов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS referrals (
                    id SERIAL PRIMARY KEY,
                    referrer_id BIGINT,
                    referred_id BIGINT UNIQUE,
                    created_at BIGINT,
                    commission_paid BOOLEAN DEFAULT FALSE,
                    payment_amount DECIMAL(12,2) DEFAULT 0,
                    currency VARCHAR(10) DEFAULT 'TON'
                )
            ''')
            try:
                await conn.execute("ALTER TABLE referrals ADD COLUMN IF NOT EXISTS currency VARCHAR(10) DEFAULT 'TON'")
            except Exception:
                pass

            # Таблица платежей
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS payments (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    amount_ton DECIMAL(12,2) DEFAULT 0,
                    amount_rub INTEGER DEFAULT 0,
                    amount_stars INTEGER DEFAULT 0,
                    plan TEXT,
                    txid TEXT,
                    status TEXT DEFAULT 'pending',
                    source TEXT DEFAULT 'ton_manual',
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
                    confirmed_at BIGINT
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

            # Таблица сообщений тикетов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS ticket_messages (
                    id SERIAL PRIMARY KEY,
                    ticket_id INTEGER REFERENCES support_tickets(id) ON DELETE CASCADE,
                    user_id BIGINT,
                    message TEXT,
                    is_from_mod BOOLEAN DEFAULT FALSE,
                    created_at BIGINT
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
                    price_value INTEGER DEFAULT 0,
                    address TEXT,
                    metro VARCHAR(100),
                    rooms VARCHAR(20),
                    floor VARCHAR(20),
                    area VARCHAR(50),
                    owner BOOLEAN,
                    district VARCHAR(50),
                    url TEXT,
                    photos JSONB,
                    published_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            ''')
            try:
                await conn.execute("ALTER TABLE ads ADD COLUMN IF NOT EXISTS price_value INTEGER DEFAULT 0")
            except Exception:
                pass

            # Таблица истории отправок
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS sent_ads (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT,
                    ad_id VARCHAR(255),
                    sent_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(user_id, ad_id)
                )
            ''')

            # Таблица балансов
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS balances (
                    user_id BIGINT,
                    currency VARCHAR(10) NOT NULL,
                    amount DECIMAL(12,2) DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (user_id, currency)
                )
            ''')

            # Индексы
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_ads_created ON ads(created_at)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_users_subscribed ON users(subscribed_until)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_payments_user_status ON payments(user_id, status)')
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_payments_txid ON payments(txid)')

        logger.info("База данных инициализирована")

    @classmethod
    async def close(cls):
        if cls._pool:
            await cls._pool.close()
            logger.info("Пул соединений закрыт")

    # ========== Пользователи ==========
    @classmethod
    async def get_user(cls, user_id: int) -> Optional[Any]:
        async with cls._pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT filters, subscribed_until, last_ad_id, plan, subscription_source, role, referrer_id FROM users WHERE user_id = $1',
                user_id
            )
            return row if row else None

    @classmethod
    async def create_user(cls, user_id: int):
        async with cls._pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING',
                user_id
            )

    @classmethod
    async def set_user_role(cls, user_id: int, role: str):
        async with cls._pool.acquire() as conn:
            await conn.execute('UPDATE users SET role = $1 WHERE user_id = $2', role, user_id)

    @classmethod
    async def set_user_referrer(cls, user_id: int, referrer_id: int):
        async with cls._pool.acquire() as conn:
            await conn.execute('UPDATE users SET referrer_id = $1 WHERE user_id = $2', referrer_id, user_id)
            await conn.execute('''
                INSERT INTO referrals (referrer_id, referred_id, created_at) VALUES ($1, $2, $3)
                ON CONFLICT (referred_id) DO NOTHING
            ''', referrer_id, user_id, int(time.time()))

    @classmethod
    async def set_user_filters(cls, user_id: int, filters_dict: dict):
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, filters) VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE SET filters = EXCLUDED.filters
            ''', user_id, json.dumps(filters_dict, ensure_ascii=False))

    @classmethod
    async def activate_subscription(cls, user_id: int, days: int, plan: str = None, source: str = 'grant'):
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
    async def update_last_ad(cls, user_id: int, ad_id: str):
        async with cls._pool.acquire() as conn:
            await conn.execute(
                'UPDATE users SET last_ad_id = $1 WHERE user_id = $2',
                ad_id, user_id
            )

    @classmethod
    async def get_referrals(cls, referrer_id: int):
        async with cls._pool.acquire() as conn:
            rows = await conn.fetch(
                'SELECT referred_id, created_at, commission_paid, payment_amount, currency FROM referrals WHERE referrer_id = $1',
                referrer_id
            )
            return rows

    # ========== Платежи ==========
    @classmethod
    async def add_payment(cls, user_id: int, amount_ton: float = 0, amount_rub: int = 0,
                          amount_stars: int = 0, plan: str = None, source: str = 'ton_manual'):
        async with cls._pool.acquire() as conn:
            return await conn.fetchval(
                '''INSERT INTO payments (user_id, amount_ton, amount_rub, amount_stars, plan, source, status)
                   VALUES ($1, $2, $3, $4, $5, $6, 'pending') RETURNING id''',
                user_id, amount_ton, amount_rub, amount_stars, plan, source
            )

    @classmethod
    async def get_pending_payment(cls, payment_id: int):
        async with cls._pool.acquire() as conn:
            return await conn.fetchrow(
                'SELECT user_id, plan, amount_ton, amount_rub, amount_stars FROM payments WHERE id = $1 AND status = $2',
                payment_id, 'pending'
            )

    @classmethod
    async def confirm_payment(cls, payment_id: int):
        async with cls._pool.acquire() as conn:
            await conn.execute(
                "UPDATE payments SET status = 'confirmed', confirmed_at = EXTRACT(EPOCH FROM NOW())::BIGINT WHERE id = $1",
                payment_id
            )

    # ========== Статистика ==========
    @classmethod
    async def get_stats(cls):
        from config import PRICES_TON, PLAN_DAYS
        now = int(time.time())
        async with cls._pool.acquire() as conn:
            total = await conn.fetchval('SELECT COUNT(*) FROM users')
            active = await conn.fetchval('SELECT COUNT(*) FROM users WHERE subscribed_until > $1', now)
            pending = await conn.fetchval("SELECT COUNT(*) FROM payments WHERE status = 'pending'")
            total_income_ton = await conn.fetchval(
                "SELECT COALESCE(SUM(amount_ton), 0) FROM payments WHERE status = 'confirmed'")
            total_income_rub = await conn.fetchval(
                "SELECT COALESCE(SUM(amount_rub), 0) FROM payments WHERE status = 'confirmed'")
            total_income_stars = await conn.fetchval(
                "SELECT COALESCE(SUM(amount_stars), 0) FROM payments WHERE status = 'confirmed'")
            active_plans = await conn.fetch(
                'SELECT plan, subscription_source FROM users WHERE subscribed_until > $1 AND plan IS NOT NULL', now)
            monthly_ton = 0.0
            for row in active_plans:
                plan = row['plan']
                if plan in PRICES_TON and plan in PLAN_DAYS:
                    monthly_ton += PRICES_TON[plan] / PLAN_DAYS[plan] * 30
            open_tickets = await conn.fetchval("SELECT COUNT(*) FROM support_tickets WHERE status = 'open'")
            ads_count = await conn.fetchval('SELECT COUNT(*) FROM ads')
            return total, active, pending, float(total_income_ton), int(total_income_rub), int(
                total_income_stars), monthly_ton, open_tickets, ads_count

    @classmethod
    async def get_all_users(cls, limit: int = 20, offset: int = 0):
        async with cls._pool.acquire() as conn:
            return await conn.fetch(
                'SELECT user_id, subscribed_until, plan, subscription_source FROM users ORDER BY user_id LIMIT $1 OFFSET $2',
                limit, offset
            )

    @classmethod
    async def get_active_subscribers(cls):
        now = int(time.time())
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT user_id, filters FROM users WHERE subscribed_until > $1', now)

    @classmethod
    async def get_active_subscribers_detailed(cls):
        now = int(time.time())
        async with cls._pool.acquire() as conn:
            return await conn.fetch('''
                SELECT user_id, subscribed_until, plan, subscription_source 
                FROM users WHERE subscribed_until > $1 ORDER BY subscribed_until DESC
            ''', now)

    # ========== Тикеты ==========
    @classmethod
    async def create_ticket(cls, user_id: int, message: str):
        created_at = int(time.time())
        async with cls._pool.acquire() as conn:
            return await conn.fetchval(
                'INSERT INTO support_tickets (user_id, message, created_at) VALUES ($1, $2, $3) RETURNING id',
                user_id, message, created_at
            )

    @classmethod
    async def add_ticket_message(cls, ticket_id: int, user_id: int, message: str, is_from_mod: bool = False):
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO ticket_messages (ticket_id, user_id, message, is_from_mod, created_at)
                VALUES ($1, $2, $3, $4, $5)
            ''', ticket_id, user_id, message, is_from_mod, int(time.time()))

    @classmethod
    async def get_ticket_messages(cls, ticket_id: int):
        async with cls._pool.acquire() as conn:
            return await conn.fetch(
                'SELECT * FROM ticket_messages WHERE ticket_id = $1 ORDER BY created_at', ticket_id)

    @classmethod
    async def get_user_open_ticket(cls, user_id: int):
        async with cls._pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id FROM support_tickets WHERE user_id = $1 AND status = 'open'", user_id)
            return row['id'] if row else None

    @classmethod
    async def get_open_tickets(cls):
        async with cls._pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM support_tickets WHERE status = 'open' ORDER BY created_at")

    @classmethod
    async def get_closed_tickets(cls, limit: int = 20, offset: int = 0):
        async with cls._pool.acquire() as conn:
            return await conn.fetch(
                "SELECT * FROM support_tickets WHERE status = 'closed' ORDER BY created_at DESC LIMIT $1 OFFSET $2",
                limit, offset
            )

    @classmethod
    async def close_ticket(cls, ticket_id: int):
        async with cls._pool.acquire() as conn:
            await conn.execute("UPDATE support_tickets SET status = 'closed' WHERE id = $1", ticket_id)

    @classmethod
    async def assign_ticket(cls, ticket_id: int, moderator_id: int):
        async with cls._pool.acquire() as conn:
            await conn.execute('UPDATE support_tickets SET assigned_to = $1 WHERE id = $2', moderator_id, ticket_id)

    # ========== Модераторы ==========
    @classmethod
    async def add_moderator(cls, user_id: int, permissions: List[str], added_by: int):
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO moderators (user_id, permissions, added_by, added_at)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (user_id) DO UPDATE SET permissions = EXCLUDED.permissions
            ''', user_id, permissions, added_by, int(time.time()))

    @classmethod
    async def remove_moderator(cls, user_id: int):
        async with cls._pool.acquire() as conn:
            await conn.execute('DELETE FROM moderators WHERE user_id = $1', user_id)

    @classmethod
    async def get_moderators(cls):
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT * FROM moderators ORDER BY added_at')

    @classmethod
    async def is_moderator(cls, user_id: int):
        async with cls._pool.acquire() as conn:
            row = await conn.fetchrow('SELECT permissions FROM moderators WHERE user_id = $1', user_id)
            return row['permissions'] if row else None

    @classmethod
    async def has_permission(cls, user_id: int, perm: str):
        perms = await cls.is_moderator(user_id)
        return perms and perm in perms

    # ========== Бан ==========
    @classmethod
    async def ban_user(cls, user_id: int):
        async with cls._pool.acquire() as conn:
            await conn.execute('UPDATE users SET banned = TRUE WHERE user_id = $1', user_id)

    @classmethod
    async def unban_user(cls, user_id: int):
        async with cls._pool.acquire() as conn:
            await conn.execute('UPDATE users SET banned = FALSE WHERE user_id = $1', user_id)

    @classmethod
    async def is_banned(cls, user_id: int) -> bool:
        async with cls._pool.acquire() as conn:
            row = await conn.fetchval('SELECT banned FROM users WHERE user_id = $1', user_id)
            return bool(row) if row is not None else False

    @classmethod
    async def get_banned_users(cls):
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT user_id FROM users WHERE banned = TRUE')

    # ========== Объявления ==========
    @classmethod
    async def save_ad(cls, ad: Ad) -> bool:
        async with cls._pool.acquire() as conn:
            try:
                result = await conn.execute('''
                    INSERT INTO ads (ad_id, source, deal_type, title, price, price_value, address, metro,
                                     rooms, floor, area, owner, district, url, photos, published_at)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
                    ON CONFLICT (ad_id) DO NOTHING
                ''', ad.id, ad.source, ad.deal_type, ad.title, ad.price, ad.price_value,
                    ad.address, ad.metro, ad.rooms, ad.floor, ad.area, ad.owner,
                    ad.district_detected, ad.link, json.dumps(ad.photos), datetime.now())
                return 'INSERT 0 1' in result
            except Exception as e:
                logger.error(f"Ошибка сохранения объявления {ad.id}: {e}")
                return False

    @classmethod
    async def was_ad_sent_to_user(cls, user_id: int, ad_id: str) -> bool:
        async with cls._pool.acquire() as conn:
            row = await conn.fetchrow(
                'SELECT id FROM sent_ads WHERE user_id = $1 AND ad_id = $2', user_id, ad_id)
            return row is not None

    @classmethod
    async def mark_ad_sent(cls, user_id: int, ad_id: str):
        async with cls._pool.acquire() as conn:
            await conn.execute(
                'INSERT INTO sent_ads (user_id, ad_id) VALUES ($1, $2) ON CONFLICT DO NOTHING',
                user_id, ad_id
            )

    # ========== Балансы ==========
    @classmethod
    async def add_to_balance(cls, user_id: int, currency: str, amount: float):
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO balances (user_id, currency, amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, currency)
                DO UPDATE SET amount = balances.amount + EXCLUDED.amount, updated_at = NOW()
            ''', user_id, currency, amount)

    @classmethod
    async def get_balance(cls, user_id: int, currency: str) -> float:
        async with cls._pool.acquire() as conn:
            row = await conn.fetchval(
                'SELECT amount FROM balances WHERE user_id=$1 AND currency=$2', user_id, currency)
            return float(row) if row else 0.0

    @classmethod
    async def deduct_from_balance(cls, user_id: int, currency: str, amount: float):
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                UPDATE balances SET amount = amount - $1, updated_at = NOW()
                WHERE user_id=$2 AND currency=$3 AND amount >= $1
            ''', amount, user_id, currency)

    @classmethod
    async def get_all_balances(cls):
        async with cls._pool.acquire() as conn:
            return await conn.fetch('SELECT * FROM balances ORDER BY user_id, currency')

    @classmethod
    async def set_balance(cls, user_id: int, currency: str, amount: float):
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO balances (user_id, currency, amount)
                VALUES ($1, $2, $3)
                ON CONFLICT (user_id, currency)
                DO UPDATE SET amount = $3, updated_at = NOW()
            ''', user_id, currency, amount)

    # ========== Очистка ==========
    @classmethod
    async def cleanup_old_ads(cls, days: int = 30):
        async with cls._pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM ads WHERE created_at < NOW() - $1::interval", timedelta(days=days))
            deleted = result.split()[-1] if result else "0"
            logger.info(f"Очистка БД: удалено {deleted} старых объявлений")