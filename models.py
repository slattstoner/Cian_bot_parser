from pydantic import BaseModel, Field, validator
from typing import Optional, List
from datetime import datetime
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

    @validator('price_value', always=True)
    def extract_price_value(cls, v, values):
        if 'price' in values and values['price'] != 'Цена не указана':
            match = re.search(r'(\d+[\s\d]*)', values['price'].replace(' ', ''))
            if match:
                return int(match.group(1).replace(' ', ''))
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