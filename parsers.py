#!/usr/bin/env python3

import asyncio
import hashlib
import json
import logging
import random
import re
from typing import Iterable, List, Optional
from urllib.parse import urlencode, urljoin

from bs4 import BeautifulSoup

try:
    import aiohttp
    AIOHTTP_AVAILABLE = True
except ImportError:
    AIOHTTP_AVAILABLE = False

try:
    from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    PlaywrightTimeoutError = Exception

from config import USER_AGENTS, PROXY_LIST, DADATA_API_KEY
from models import Ad

logger = logging.getLogger(__name__)

CIAN_WAIT_SELECTORS = [
    'article[data-name]',
    '[data-name="CardComponent"]',
    '[data-testid="offer-card"]',
    'a[href*="/sale/flat/"]',
    'a[href*="/rent/flat/"]',
]

AVITO_WAIT_SELECTORS = [
    '[data-marker="item"]',
    'a[data-marker="item-title"]',
    '[itemtype="http://schema.org/Product"]',
    'a[href*="/kvartiry/"]',
]


async def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)


async def get_random_proxy() -> Optional[str]:
    return random.choice(PROXY_LIST) if PROXY_LIST else None


def _cleanup_text(value: str) -> str:
    return re.sub(r'\s+', ' ', (value or '').strip())


def _extract_price_value(price: str) -> int:
    digits = re.sub(r'[^\d]', '', price or '')
    return int(digits) if digits else 0


def _extract_rooms(title: str, body: str) -> str:
    merged = f"{title} {body}".lower()
    if 'студ' in merged:
        return 'студия'
    match = re.search(r'(\d+)\s*[- ]?комн', merged)
    return match.group(1) if match else '?'


def _extract_floor(body: str) -> str:
    match = re.search(r'(\d+)\s*/\s*(\d+)\s*эт', body.lower())
    if not match:
        match = re.search(r'(\d+)\s*этаж\s*из\s*(\d+)', body.lower())
    return f"{match.group(1)}/{match.group(2)}" if match else '?/?'


def _extract_area(body: str) -> str:
    match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', body.lower())
    return f"{match.group(1).replace('.', ',')} м²" if match else '? м²'


def _extract_images(card) -> list[str]:
    photos = []
    for img in card.find_all('img', src=True)[:6]:
        src = img.get('src', '').strip()
        if src.startswith('//'):
            src = 'https:' + src
        if src.startswith('http') and not src.endswith('.svg') and 'avatar' not in src and 'placeholder' not in src:
            photos.append(src)
    return list(dict.fromkeys(photos))


async def get_page_html_playwright(
    url: str,
    params: dict | None = None,
    use_proxy: bool = True,
    wait_selectors: list[str] | None = None,
    debug_name: str = 'page',
) -> Optional[str]:
    if not PLAYWRIGHT_AVAILABLE:
        logger.warning('Playwright не установлен')
        return None

    full_url = f"{url}?{urlencode(params)}" if params else url

    for attempt in range(1, 4):
        browser = context = page = None
        try:
            async with async_playwright() as p:
                launch_kwargs = {
                    'headless': True,
                    'args': [
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--window-size=1600,1200',
                    ]
                }
                proxy = await get_random_proxy() if use_proxy else None
                if proxy:
                    launch_kwargs['proxy'] = {'server': proxy}

                browser = await p.chromium.launch(**launch_kwargs)
                context = await browser.new_context(
                    user_agent=await get_random_user_agent(),
                    viewport={'width': 1600, 'height': 1200},
                    locale='ru-RU',
                    timezone_id='Europe/Moscow',
                )
                page = await context.new_page()
                logger.info('Загрузка %s attempt=%s proxy=%s', full_url, attempt, proxy or 'none')

                response = await page.goto(full_url, wait_until='domcontentloaded', timeout=60000)
                status = response.status if response else None
                await page.wait_for_timeout(random.randint(2000, 3500))

                for percent in (0.35, 0.7, 1.0):
                    await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {percent})")
                    await page.wait_for_timeout(random.randint(1000, 2000))

                if wait_selectors:
                    for selector in wait_selectors:
                        try:
                            await page.wait_for_selector(selector, timeout=7000)
                            logger.info('%s: найден селектор %s', debug_name, selector)
                            break
                        except PlaywrightTimeoutError:
                            continue

                html = await page.content()
                title = await page.title()
                logger.info('%s status=%s final_url=%s title=%r html_len=%s', debug_name, status, page.url, title, len(html))
                with open(f'/tmp/{debug_name}_debug.html', 'w', encoding='utf-8') as fh:
                    fh.write(html)
                lowered = html.lower()
                for marker in ('captcha', 'robot', 'verify', 'access denied', 'blocked'):
                    if marker in lowered:
                        logger.warning('%s: найден антибот маркер %r', debug_name, marker)
                return html
        except Exception:
            logger.exception('Ошибка загрузки %s attempt=%s', full_url, attempt)
            await asyncio.sleep(attempt * 2)
        finally:
            try:
                if page:
                    await page.close()
            except Exception:
                pass
            try:
                if context:
                    await context.close()
            except Exception:
                pass
            try:
                if browser:
                    await browser.close()
            except Exception:
                pass
    return None


def _iter_ld_json(soup: BeautifulSoup) -> Iterable[dict]:
    for script in soup.find_all('script', type='application/ld+json'):
        raw = (script.string or script.get_text() or '').strip()
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        if isinstance(data, dict):
            yield data
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    yield item


def _find_cards(soup: BeautifulSoup, selectors: list[tuple[str, dict]]) -> list:
    for tag, attrs in selectors:
        found = soup.find_all(tag, attrs)
        if found:
            return found
    return []


async def fetch_cian_deal_type(deal_type: str = 'sale') -> List[Ad]:
    params = {
        'deal_type': deal_type,
        'engine_version': '2',
        'offer_type': 'flat',
        'region': '1',
        'sort': 'creation_date_desc',
        'p': '1',
    }
    html = await get_page_html_playwright(
        'https://www.cian.ru/cat.php',
        params,
        use_proxy=True,
        wait_selectors=CIAN_WAIT_SELECTORS,
        debug_name=f'cian_{deal_type}',
    )
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    cards = _find_cards(soup, [
        ('article', {'data-name': re.compile('CardComponent')}),
        ('div', {'data-testid': 'offer-card'}),
        ('div', {'class': re.compile('offer-card|_93444fe79c--container')}),
    ])
    logger.info('ЦИАН (%s): найдено %s карточек', deal_type, len(cards))

    results: list[Ad] = []
    seen_ids: set[str] = set()

    for card in cards[:40]:
        try:
            link_tag = card.find('a', href=True)
            if not link_tag:
                continue
            link = urljoin('https://www.cian.ru', link_tag['href'])
            ad_id_match = re.search(r'/([0-9]+)/?(?:\?.*)?$', link)
            ad_id = f'cian_{ad_id_match.group(1)}' if ad_id_match else f'cian_{hashlib.md5(link.encode()).hexdigest()}'
            if ad_id in seen_ids:
                continue
            seen_ids.add(ad_id)

            body = _cleanup_text(card.get_text(' ', strip=True))
            title = _cleanup_text((card.find('h3') or link_tag).get_text(' ', strip=True) or 'Квартира')

            price_tag = card.find('span', {'data-mark': 'MainPrice'}) or card.find('meta', {'itemprop': 'price'}) or card.find(string=re.compile(r'₽'))
            if getattr(price_tag, 'name', None) == 'meta':
                price = f"{price_tag.get('content', '').strip()} ₽"
            else:
                price = _cleanup_text(price_tag.get_text(' ', strip=True) if hasattr(price_tag, 'get_text') else str(price_tag or 'Цена не указана'))

            address = _cleanup_text((card.find('address') or card.find(string=re.compile(r'Москва')) or 'Москва').get_text(' ', strip=True) if hasattr(card.find('address'), 'get_text') else (card.find('address').get_text(' ', strip=True) if card.find('address') else 'Москва'))
            if address == 'Москва' and (addr_el := card.find('address')):
                address = _cleanup_text(addr_el.get_text(' ', strip=True))

            metro_candidates = []
            for el in card.find_all(['a', 'span'], href=True) + card.find_all('span'):
                txt = _cleanup_text(el.get_text(' ', strip=True))
                if txt and ('м.' in txt.lower() or 'метро' in txt.lower() or 'мин' in txt.lower()):
                    metro_candidates.append(txt)
            metro = ' | '.join(dict.fromkeys(metro_candidates[:3])) if metro_candidates else 'Не указано'

            ad = Ad(
                id=ad_id,
                source='cian',
                deal_type=deal_type,
                title=title,
                link=link,
                price=price,
                address=address,
                metro=metro,
                floor=_extract_floor(body),
                area=_extract_area(body),
                rooms=_extract_rooms(title, body),
                owner=('собственник' in body.lower() or 'без посредников' in body.lower()),
                photos=_extract_images(card),
                district_detected=await get_district_by_address(address),
                price_value=_extract_price_value(price),
            )
            results.append(ad)
        except Exception:
            logger.exception('Ошибка парсинга карточки ЦИАН')

    return results


async def fetch_avito_deal_type(deal_type: str = 'sale') -> List[Ad]:
    category = 'prodazha-kvartir' if deal_type == 'sale' else 'snyat-kvartiru'
    html = await get_page_html_playwright(
        f'https://www.avito.ru/moskva/kvartiry/{category}',
        {'s': '1', 'p': '1'},
        use_proxy=True,
        wait_selectors=AVITO_WAIT_SELECTORS,
        debug_name=f'avito_{deal_type}',
    )
    if not html:
        return []

    soup = BeautifulSoup(html, 'lxml')
    cards = _find_cards(soup, [
        ('div', {'data-marker': 'item'}),
        ('div', {'itemtype': 'http://schema.org/Product'}),
        ('div', {'class': re.compile('iva-item')}),
    ])
    logger.info('Авито (%s): найдено %s карточек', deal_type, len(cards))

    results: list[Ad] = []
    seen_ids: set[str] = set()

    for card in cards[:40]:
        try:
            link_tag = card.find('a', {'data-marker': 'item-title'}) or card.find('a', href=True)
            if not link_tag:
                continue
            link = urljoin('https://www.avito.ru', link_tag.get('href', ''))
            ad_id_match = re.search(r'/([0-9]+)(?:[/?#]|$)', link)
            ad_id = f'avito_{ad_id_match.group(1)}' if ad_id_match else f'avito_{hashlib.md5(link.encode()).hexdigest()}'
            if ad_id in seen_ids:
                continue
            seen_ids.add(ad_id)

            body = _cleanup_text(card.get_text(' ', strip=True))
            title = _cleanup_text((card.find('h3') or link_tag).get_text(' ', strip=True) or 'Квартира')

            price_meta = card.find('meta', {'itemprop': 'price'})
            if price_meta:
                price = f"{price_meta.get('content', '').strip()} ₽"
            else:
                price_tag = card.find('span', {'data-marker': 'item-price'}) or card.find(string=re.compile(r'₽'))
                price = _cleanup_text(price_tag.get_text(' ', strip=True) if hasattr(price_tag, 'get_text') else str(price_tag or 'Цена не указана'))

            address_el = card.find('span', {'data-marker': 'item-address'})
            address = _cleanup_text(address_el.get_text(' ', strip=True) if address_el else 'Москва')

            metro_el = card.find('span', {'data-marker': 'item-metro'})
            metro = _cleanup_text(metro_el.get_text(' ', strip=True) if metro_el else 'Не указано').replace('м.', '').strip()

            ad = Ad(
                id=ad_id,
                source='avito',
                deal_type=deal_type,
                title=title,
                link=link,
                price=price,
                address=address,
                metro=metro,
                floor=_extract_floor(body),
                area=_extract_area(body),
                rooms=_extract_rooms(title, body),
                owner=any(word in body.lower() for word in ('собственник', 'без посредников', 'частное лицо')),
                photos=_extract_images(card),
                district_detected=await get_district_by_address(address),
                price_value=_extract_price_value(price),
            )
            results.append(ad)
        except Exception:
            logger.exception('Ошибка парсинга карточки Авито')

    return results


async def fetch_all_ads() -> List[Ad]:
    tasks = [
        ('cian_sale', fetch_cian_deal_type('sale')),
        ('cian_rent', fetch_cian_deal_type('rent')),
        ('avito_sale', fetch_avito_deal_type('sale')),
        ('avito_rent', fetch_avito_deal_type('rent')),
    ]
    all_ads: list[Ad] = []
    for name, coro in tasks:
        try:
            result = await coro
            all_ads.extend(result)
        except Exception:
            logger.exception('Ошибка в парсере %s', name)

    unique: list[Ad] = []
    seen: set[str] = set()
    for ad in all_ads:
        if ad.id not in seen:
            seen.add(ad.id)
            unique.append(ad)
    logger.info('Всего собрано %s уникальных объявлений', len(unique))
    return unique


async def get_district_by_address(address: str) -> Optional[str]:
    if not (DADATA_API_KEY and AIOHTTP_AVAILABLE and address):
        return None
    url = 'https://cleaner.dadata.ru/api/v1/clean/address'
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Token {DADATA_API_KEY}',
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=[address], timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                payload = await resp.json()
        if payload and isinstance(payload, list):
            item = payload[0]
            area = item.get('area')
            area_type = item.get('area_type')
            if area and area_type == 'округ':
                return area.upper()
    except Exception:
        logger.debug('DaData не смог определить округ', exc_info=True)
    return None
