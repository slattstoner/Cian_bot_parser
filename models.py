#!/usr/bin/env python3
# models.py v1.1 (04.03.2026)
# - Без изменений, только версия

from pydantic import BaseModel, Field, validator
from typing import Optional, List
import re


class Ad(BaseModel):
    id: str
    source: str = 'cian'
    deal_type: str = 'sale'
    title: str
    link: str
    price: str
    address: str
    metro: str
    floor: str
    area: str
    rooms: str
    owner: bool = False
    photos: List[str] = []
    district_detected: Optional[str] = None
    price_value: int = 0

    @validator('price_value', always=True, pre=True)
    def extract_price_value(cls, v, values):
        if v:
            return v
        price = values.get('price', '')
        if price and price != 'Цена не указана':
            cleaned = re.sub(r'[^\d]', '', price)
            if cleaned:
                try:
                    return int(cleaned)
                except:
                    pass
        return 0


class UserFilters(BaseModel):
    city: str = 'Москва'
    districts: List[str] = []
    rooms: List[str] = []
    metros: List[str] = []
    owner_only: bool = False
    deal_type: str = 'sale'
    sources: List[str] = ['cian', 'avito']


class Payment(BaseModel):
    id: int
    user_id: int
    amount_ton: float = 0
    amount_rub: int = 0
    amount_stars: int = 0
    plan: Optional[str] = None
    txid: Optional[str] = None
    status: str = 'pending'
    source: str = 'ton_manual'
    created_at: int
    confirmed_at: Optional[int] = None


class User(BaseModel):
    user_id: int
    role: str = 'user'
    referrer_id: Optional[int] = None
    filters: Optional[UserFilters] = None
    subscribed_until: Optional[int] = None
    last_ad_id: Optional[str] = None
    plan: Optional[str] = None
    subscription_source: Optional[str] = None
    created_at: int