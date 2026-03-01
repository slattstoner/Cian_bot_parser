import asyncpg
import json
import time
import logging
from typing import Optional, List, Tuple, Any
from datetime import datetime, timedelta
from models import Ad, UserFilters

logger = logging.getLogger(__name__)

class Database:
    _pool: Optional[asyncpg.Pool] = None

    @classmethod
    async def init(cls, dsn: str):
        cls._pool = await asyncpg.create_pool(dsn, min_size=5, max_size=20)
        async with cls._pool.acquire() as conn:
            # ... (все CREATE TABLE как в оригинале, добавим индексы)
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
                    created_at BIGINT DEFAULT EXTRACT(EPOCH FROM NOW())
                )
            ''')
            # ... остальные таблицы (копируем из оригинального кода)
            # добавим индекс на created_at в ads для быстрой очистки
            await conn.execute('CREATE INDEX IF NOT EXISTS idx_ads_created ON ads(created_at)')
        logger.info("База данных инициализирована")

    @classmethod
    async def close(cls):
        if cls._pool:
            await cls._pool.close()
            logger.info("Пул соединений закрыт")

    # Методы для пользователей
    @classmethod
    async def get_user(cls, user_id: int) -> Optional[Tuple]:
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
    async def set_user_filters(cls, user_id: int, filters: UserFilters):
        async with cls._pool.acquire() as conn:
            await conn.execute('''
                INSERT INTO users (user_id, filters) VALUES ($1, $2)
                ON CONFLICT (user_id) DO UPDATE SET filters = EXCLUDED.filters
            ''', user_id, filters.json())

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

    # ... остальные методы (get_referrals, update_last_ad, add_payment, get_pending_payment, confirm_payment, get_stats, get_all_users, get_active_subscribers, get_active_subscribers_detailed, create_ticket, get_open_tickets, close_ticket, assign_ticket, add_moderator, remove_moderator, get_moderators, is_moderator, has_permission, save_ad, was_ad_sent_to_user, mark_ad_sent, add_to_balance, get_balance, deduct_from_balance, get_all_balances)
    # Их нужно скопировать из оригинального кода, заменив при необходимости использование json.dumps на model.json() или model.dict()
    # В целях экономии места я не привожу их все здесь, но они должны быть вставлены.

    @classmethod
    async def save_ad(cls, ad: Ad) -> bool:
        async with cls._pool.acquire() as conn:
            try:
                result = await conn.execute('''
                    INSERT INTO ads (ad_id, source, deal_type, title, price, price_value, address, metro, rooms, floor, area, owner, district, url, photos, published_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                    ON CONFLICT (ad_id) DO NOTHING
                ''', ad.id, ad.source, ad.deal_type, ad.title, ad.price, ad.price_value, ad.address, ad.metro, ad.rooms,
                   ad.floor, ad.area, ad.owner, ad.district_detected, ad.link, json.dumps(ad.photos), datetime.now())
                return 'INSERT 0 1' in result
            except Exception as e:
                logger.error(f"Ошибка сохранения объявления {ad.id}: {e}")
                return False

    # Пакетное сохранение объявлений (для оптимизации)
    @classmethod
    async def save_ads_batch(cls, ads: List[Ad]) -> int:
        if not ads:
            return 0
        async with cls._pool.acquire() as conn:
            # Используем executemany для вставки нескольких записей
            records = [(
                ad.id, ad.source, ad.deal_type, ad.title, ad.price, ad.price_value,
                ad.address, ad.metro, ad.rooms, ad.floor, ad.area, ad.owner,
                ad.district_detected, ad.link, json.dumps(ad.photos), datetime.now()
            ) for ad in ads]
            try:
                await conn.executemany('''
                    INSERT INTO ads (ad_id, source, deal_type, title, price, price_value, address, metro, rooms, floor, area, owner, district, url, photos, published_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                    ON CONFLICT (ad_id) DO NOTHING
                ''', records)
                return len(ads)
            except Exception as e:
                logger.error(f"Ошибка пакетного сохранения: {e}")
                return 0

    # Очистка старых объявлений с логированием количества
    @classmethod
    async def cleanup_old_ads(cls, days=30):
        async with cls._pool.acquire() as conn:
            result = await conn.execute('''
                DELETE FROM ads WHERE created_at < NOW() - $1::interval
            ''', timedelta(days=days))
            # result имеет формат "DELETE <count>"
            deleted = result.split()[-1] if result else "0"
            logger.info(f"Очистка БД: удалено {deleted} старых объявлений")