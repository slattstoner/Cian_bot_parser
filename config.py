import os
from typing import Dict, List, Any

# Токены и ключи
TOKEN = os.environ.get('TOKEN')
ADMIN_ID = int(os.environ.get('ADMIN_ID', 0))
TON_WALLET = os.environ.get('TON_WALLET', '')
DADATA_API_KEY = os.environ.get('DADATA_API_KEY', '')
DATABASE_URL = os.environ.get('DATABASE_URL')
PROXY_URL = os.environ.get('PROXY_URL', None)
PAYMENT_PROVIDER_TOKEN = os.environ.get('PAYMENT_PROVIDER_TOKEN', None)
GITHUB_TOKEN = os.environ.get('GITHUB_TOKEN', None)

# Репозиторий для автообновления
GITHUB_REPO = "slattstoner/Cian_bot_parser"
GITHUB_BRANCH = "main"
AUTO_UPDATE_CHECK_INTERVAL = 120  # секунд

# Цены подписок
PRICES_RUB = {'1m': 150, '3m': 400, '6m': 750, '12m': 1400}
PRICES_TON = {'1m': 1.5, '3m': 4.0, '6m': 7.5, '12m': 14.0}
PRICES_STARS = {'1m': 200, '3m': 500, '6m': 950, '12m': 1800}
PLAN_DAYS = {'1m': 30, '3m': 90, '6m': 180, '12m': 360}

# Комиссия реферера (%)
REFERRAL_COMMISSION = 50

# Настройки парсинга
PARSING_INTERVAL = 600  # 10 минут
TELEGRAM_RATE_LIMIT = 20
PROXY_LIST = PROXY_URL.split(',') if PROXY_URL else []

# Округа Москвы
DISTRICTS = ['ЦАО', 'САО', 'СВАО', 'ВАО', 'ЮВАО', 'ЮАО', 'ЮЗАО', 'ЗАО', 'СЗАО']

# Варианты комнат
ROOM_OPTIONS = ['Студия', '1-комнатная', '2-комнатная', '3-комнатная', '4-комнатная+']

# Типы сделок
DEAL_TYPES = ['sale', 'rent']
DEAL_TYPE_NAMES = {'sale': '🏠 Продажа', 'rent': '🔑 Аренда'}

# User-Agent пул
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:135.0) Gecko/20100101 Firefox/135.0'
]

# Данные по метро (заполните полный список из вашего старого кода)
METRO_LINES: Dict[str, Dict[str, Any]] = {
    '1': {'name': 'Сокольническая линия', 'stations': ['Бульвар Рокоссовского', 'Черкизовская', 'Преображенская площадь', 'Сокольники', 'Красносельская', 'Комсомольская', 'Красные Ворота', 'Чистые пруды', 'Лубянка', 'Охотный Ряд', 'Библиотека имени Ленина', 'Кропоткинская', 'Парк культуры', 'Фрунзенская', 'Спортивная', 'Воробьёвы горы', 'Университет', 'Проспект Вернадского', 'Юго-Западная', 'Тропарёво', 'Румянцево', 'Саларьево', 'Филатов Луг', 'Прокшино', 'Ольховая', 'Новомосковская']},
    '2': {'name': 'Замоскворецкая линия', 'stations': ['Ховрино', 'Беломорская', 'Речной вокзал', 'Водный стадион', 'Войковская', 'Сокол', 'Аэропорт', 'Динамо', 'Белорусская', 'Маяковская', 'Тверская', 'Театральная', 'Новокузнецкая', 'Павелецкая', 'Автозаводская', 'Технопарк', 'Коломенская', 'Каширская', 'Кантемировская', 'Царицыно', 'Орехово', 'Домодедовская', 'Красногвардейская', 'Алма-Атинская']},
    # ... добавьте все линии из вашего старого кода
}
ALL_METRO_STATIONS = [station for line in METRO_LINES.values() for station in line['stations']]