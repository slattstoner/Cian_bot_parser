#!/usr/bin/env python3
# parsers.py v2.0.0 (11.03.2026)
# - Переписан загрузчик страниц: устойчивое ожидание, debug HTML/PNG, признаки антибота
# - Добавлены несколько стратегий извлечения: JSON-LD, HTML-карточки, резервный парсинг ссылок
# - Усилена нормализация метро, адресов, цен и фото
# - Парсер работает последовательно и аккуратнее для VPS/антибота

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import random
import re
from pathlib import Path
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

__version__ = '2.0.0'

logger = logging.getLogger(__name__)
DEBUG_DIR = Path('/tmp/bot_parser_debug')
DEBUG_DIR.mkdir(parents=True, exist_ok=True)

CIAN_WAIT_SELECTORS = [
    'article[data-name]',
    '[data-name="CardComponent"]',
    '[data-testid="offer-card"]',
    'a[href*="/sale/flat/"]',
    'a[href*="/rent/flat/"]',
    'script[type="application/ld+json"]',
]

AVITO_WAIT_SELECTORS = [
    '[data-marker="item"]',
    'a[data-marker="item-title"]',
    '[itemtype="http://schema.org/Product"]',
    'a[href*="/kvartiry/"]',
    'script[type="application/ld+json"]',
]

ANTIBOT_MARKERS = (
    'captcha', 'robot', 'access denied', 'blocked', 'verify you are human',
    'доступ ограничен', 'подтвердите, что вы не робот', 'проверка браузера'
)


async def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)


async def get_random_proxy() -> Optional[str]:
    return random.choice(PROXY_LIST) if PROXY_LIST else None


class PageFetchError(RuntimeError):
    pass


def _cleanup_text(value: str | None) -> str:
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


def _unique_list(items: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        norm = _cleanup_text(item)
        if norm and norm not in seen:
            seen.add(norm)
            out.append(norm)
    return out


def _extract_images_from_tag(tag) -> list[str]:
    photos: list[str] = []
    if not tag:
        return photos
    for img in tag.find_all('img')[:8]:
        for attr in ('src', 'data-src', 'srcset'):
            raw = (img.get(attr) or '').strip()
            if not raw:
                continue
            if attr == 'srcset':
                raw = raw.split(',')[0].strip().split(' ')[0]
            if raw.startswith('//'):
                raw = 'https:' + raw
            if raw.startswith('http') and not raw.endswith('.svg') and 'placeholder' not in raw and raw not in photos:
                photos.append(raw)
                break
    return photos[:6]


def _extract_offer_from_json_ld(item: dict, source: str, deal_type: str) -> Optional[Ad]:
    if not isinstance(item, dict):
        return None
    item_type = str(item.get('@type', '')).lower()
    if item_type and item_type not in {'product', 'offer', 'apartment', 'residence', 'singlefamilyresidence', 'house'}:
        return None

    link = item.get('url') or item.get('mainEntityOfPage')
    if isinstance(link, dict):
        link = link.get('@id') or link.get('url')
    if not link or not isinstance(link, str):
        return None
    link = urljoin('https://www.cian.ru' if source == 'cian' else 'https://www.avito.ru', link)

    offer = item.get('offers') if isinstance(item.get('offers'), dict) else {}
    price_raw = offer.get('price') or item.get('price') or ''
    currency = offer.get('priceCurrency') or 'RUB'
    price = f"{price_raw} ₽" if price_raw else 'Цена не указана'
    if currency not in {'RUB', '₽'} and price_raw:
        price = f"{price_raw} {currency}"

    address = 'Москва'
    address_obj = item.get('address')
    if isinstance(address_obj, dict):
        address_parts = [
            address_obj.get('streetAddress'), address_obj.get('addressLocality'),
            address_obj.get('addressRegion'), address_obj.get('addressCountry')
        ]
        address = _cleanup_text(', '.join([x for x in address_parts if x])) or address
    elif isinstance(address_obj, str):
        address = _cleanup_text(address_obj)

    title = _cleanup_text(item.get('name') or item.get('description') or 'Квартира')
    body = _cleanup_text(item.get('description') or title)

    photos: list[str] = []
    image = item.get('image')
    if isinstance(image, list):
        photos = [urljoin(link, str(x)) for x in image if isinstance(x, str)][:6]
    elif isinstance(image, str):
        photos = [urljoin(link, image)]

    ad_id_match = re.search(r'/([0-9]+)(?:[/?#]|$)', link)
    ad_id = f'{source}_{ad_id_match.group(1)}' if ad_id_match else f'{source}_{hashlib.md5(link.encode()).hexdigest()}'

    metro_parts: list[str] = []
    for candidate in [item.get('keywords'), item.get('category'), item.get('description')]:
        if isinstance(candidate, str) and ('метро' in candidate.lower() or 'м.' in candidate.lower()):
            metro_parts.append(candidate)
    metro = _cleanup_text(' | '.join(_unique_list(metro_parts))) or 'Не указано'

    return Ad(
        id=ad_id,
        source=source,
        deal_type=deal_type,
        title=title,
        link=link,
        price=price,
        address=address,
        metro=metro,
        floor=_extract_floor(body),
        area=_extract_area(body),
        rooms=_extract_rooms(title, body),
        owner='собственник' in body.lower() or 'без посредников' in body.lower(),
        photos=photos,
        district_detected=None,
        price_value=_extract_price_value(price),
    )


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
                proxy = await get_random_proxy() if use_proxy else None
                browser = await p.chromium.launch(
                    headless=True,
                    proxy={'server': proxy} if proxy else None,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--window-size=1600,1200',
                    ],
                )
                context = await browser.new_context(
                    user_agent=await get_random_user_agent(),
                    viewport={'width': 1600, 'height': 1200},
                    locale='ru-RU',
                    timezone_id='Europe/Moscow',
                    java_script_enabled=True,
                    ignore_https_errors=True,
                )
                page = await context.new_page()
                await page.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
                    window.chrome = { runtime: {} };
                    Object.defineProperty(navigator, 'languages', {get: () => ['ru-RU', 'ru', 'en-US', 'en']});
                    Object.defineProperty(navigator, 'plugins', {get: () => [1, 2, 3, 4]});
                """)

                logger.info('Загрузка %s attempt=%s proxy=%s', full_url, attempt, proxy or 'none')
                response = await page.goto(full_url, wait_until='domcontentloaded', timeout=70000)
                status = response.status if response else None
                await page.wait_for_timeout(random.randint(2200, 3800))

                for percent in (0.25, 0.55, 0.85, 1.0):
                    await page.evaluate(f"window.scrollTo(0, document.body.scrollHeight * {percent})")
                    await page.wait_for_timeout(random.randint(900, 1800))

                selector_found = False
                if wait_selectors:
                    for selector in wait_selectors:
                        try:
                            await page.wait_for_selector(selector, timeout=6500)
                            selector_found = True
                            logger.info('%s: дождались селектор %s', debug_name, selector)
                            break
                        except PlaywrightTimeoutError:
                            logger.info('%s: селектор не найден %s', debug_name, selector)
                try:
                    await page.wait_for_load_state('networkidle', timeout=5000)
                except Exception:
                    pass
                await page.wait_for_timeout(random.randint(1200, 2200))

                title = await page.title()
                html = await page.content()
                html_path = DEBUG_DIR / f'{debug_name}_debug.html'
                png_path = DEBUG_DIR / f'{debug_name}_debug.png'
                html_path.write_text(html, encoding='utf-8')
                try:
                    await page.screenshot(path=str(png_path), full_page=True)
                except Exception:
                    logger.debug('Не удалось сохранить screenshot для %s', debug_name, exc_info=True)

                lowered = html.lower()
                antibot_found = [marker for marker in ANTIBOT_MARKERS if marker in lowered]
                logger.info(
                    '%s status=%s final_url=%s title=%r html_len=%s selector_found=%s antibot=%s html_file=%s',
                    debug_name, status, page.url, title, len(html), selector_found, ','.join(antibot_found) or 'no', html_path,
                )
                if status and status >= 400:
                    raise PageFetchError(f'HTTP status={status}')
                if antibot_found and not selector_found:
                    raise PageFetchError(f'Anti-bot page detected: {antibot_found}')
                return html
        except Exception as exc:
            logger.exception('Ошибка загрузки %s attempt=%s: %s', full_url, attempt, exc)
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


def _extract_cian_address(card) -> str:
    address = ''
    address_el = card.find('address')
    if address_el:
        address = _cleanup_text(address_el.get_text(' ', strip=True))
    if not address:
        for candidate in card.find_all(['div', 'span', 'a']):
            text = _cleanup_text(candidate.get_text(' ', strip=True))
            if text and ('москва' in text.lower() or 'район' in text.lower() or 'ул.' in text.lower()):
                address = text
                break
    return address or 'Москва'


def _extract_metro_text(card) -> str:
    metro_candidates: list[str] = []
    for el in card.find_all(['a', 'span', 'div']):
        txt = _cleanup_text(el.get_text(' ', strip=True))
        lower = txt.lower()
        if not txt:
            continue
        if 'метро' in lower or 'м.' in lower or re.search(r'\d+\s*мин', lower):
            metro_candidates.append(txt)
    return ' | '.join(_unique_list(metro_candidates[:5])) or 'Не указано'


def _extract_title(card) -> str:
    for tag in ('h3', 'h2', 'strong'):
        el = card.find(tag)
        if el:
            text = _cleanup_text(el.get_text(' ', strip=True))
            if text:
                return text
    link = card.find('a', href=True)
    if link:
        return _cleanup_text(link.get_text(' ', strip=True)) or 'Квартира'
    return 'Квартира'


def _extract_price(card) -> str:
    meta = card.find('meta', {'itemprop': 'price'})
    if meta and meta.get('content'):
        return f"{meta.get('content').strip()} ₽"
    for selector in [
        ('span', {'data-mark': 'MainPrice'}),
        ('span', {'data-marker': 'item-price'}),
    ]:
        el = card.find(*selector)
        if el:
            text = _cleanup_text(el.get_text(' ', strip=True))
            if text:
                return text
    ruble_text = card.find(string=re.compile(r'₽'))
    if ruble_text:
        return _cleanup_text(str(ruble_text))
    return 'Цена не указана'


def _extract_link(card, source: str) -> Optional[str]:
    preferred = card.find('a', {'data-marker': 'item-title'}) or card.find('a', href=True)
    if not preferred:
        return None
    base = 'https://www.cian.ru' if source == 'cian' else 'https://www.avito.ru'
    return urljoin(base, preferred.get('href', ''))


def _ad_id_from_link(link: str, source: str) -> str:
    match = re.search(r'/([0-9]+)(?:[/?#]|$)', link)
    if match:
        return f'{source}_{match.group(1)}'
    return f'{source}_{hashlib.md5(link.encode()).hexdigest()}'


def _extract_ads_from_cards(cards: list, source: str, deal_type: str) -> list[Ad]:
    results: list[Ad] = []
    seen: set[str] = set()
    for card in cards[:60]:
        try:
            link = _extract_link(card, source)
            if not link:
                continue
            ad_id = _ad_id_from_link(link, source)
            if ad_id in seen:
                continue
            seen.add(ad_id)
            body = _cleanup_text(card.get_text(' ', strip=True))
            title = _extract_title(card)
            price = _extract_price(card)
            address = _extract_cian_address(card) if source == 'cian' else _cleanup_text((card.find('span', {'data-marker': 'item-address'}) or card).get_text(' ', strip=True))
            metro = _extract_metro_text(card)
            owner = any(word in body.lower() for word in ('собственник', 'без посредников', 'частное лицо'))
            results.append(Ad(
                id=ad_id,
                source=source,
                deal_type=deal_type,
                title=title,
                link=link,
                price=price,
                address=address or 'Москва',
                metro=metro,
                floor=_extract_floor(body),
                area=_extract_area(body),
                rooms=_extract_rooms(title, body),
                owner=owner,
                photos=_extract_images_from_tag(card),
                district_detected=None,
                price_value=_extract_price_value(price),
            ))
        except Exception:
            logger.exception('Ошибка парсинга карточки source=%s', source)
    return results


def _extract_ads_from_json_ld(soup: BeautifulSoup, source: str, deal_type: str) -> list[Ad]:
    results: list[Ad] = []
    seen: set[str] = set()
    for item in _iter_ld_json(soup):
        ad = _extract_offer_from_json_ld(item, source, deal_type)
        if ad and ad.id not in seen:
            seen.add(ad.id)
            results.append(ad)
    return results


def _merge_ads(*collections: list[Ad]) -> list[Ad]:
    merged: list[Ad] = []
    seen: set[str] = set()
    for collection in collections:
        for ad in collection:
            if ad.id in seen:
                continue
            seen.add(ad.id)
            merged.append(ad)
    return merged


async def _finalize_ads(ads: list[Ad]) -> list[Ad]:
    for ad in ads:
        if not ad.district_detected:
            ad.district_detected = await get_district_by_address(ad.address)
    return ads


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
    card_ads = _extract_ads_from_cards(_find_cards(soup, [
        ('article', {'data-name': re.compile('CardComponent', re.I)}),
        ('div', {'data-testid': 'offer-card'}),
        ('div', {'class': re.compile('offer-card|_93444fe79c--container', re.I)}),
    ]), 'cian', deal_type)
    json_ads = _extract_ads_from_json_ld(soup, 'cian', deal_type)
    ads = _merge_ads(json_ads, card_ads)
    logger.info('ЦИАН (%s): json=%s cards=%s merged=%s', deal_type, len(json_ads), len(card_ads), len(ads))
    return await _finalize_ads(ads)


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
    card_ads = _extract_ads_from_cards(_find_cards(soup, [
        ('div', {'data-marker': 'item'}),
        ('div', {'itemtype': 'http://schema.org/Product'}),
        ('div', {'class': re.compile('iva-item', re.I)}),
        ('article', {'data-marker': re.compile('item', re.I)}),
    ]), 'avito', deal_type)
    json_ads = _extract_ads_from_json_ld(soup, 'avito', deal_type)
    ads = _merge_ads(json_ads, card_ads)
    logger.info('Авито (%s): json=%s cards=%s merged=%s', deal_type, len(json_ads), len(card_ads), len(ads))
    return await _finalize_ads(ads)


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
            part = await coro
            logger.info('%s: получено %s объявлений', name, len(part))
            all_ads.extend(part)
            await asyncio.sleep(random.uniform(1.0, 2.4))
        except Exception:
            logger.exception('Ошибка в парсере %s', name)
    unique = _merge_ads(all_ads)
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
