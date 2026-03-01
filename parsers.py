import asyncio
import random
import re
import hashlib
import logging
from typing import List, Optional
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import aiohttp

from config import USER_AGENTS, PROXY_LIST, DADATA_API_KEY, DISTRICTS
from models import Ad

logger = logging.getLogger(__name__)

async def get_random_user_agent() -> str:
    return random.choice(USER_AGENTS)

async def get_random_proxy() -> Optional[str]:
    if PROXY_LIST:
        return random.choice(PROXY_LIST)
    return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    retry=retry_if_exception_type(Exception),
    reraise=True
)
async def get_page_html_playwright(url: str, params: dict = None, use_proxy: bool = True) -> Optional[str]:
    """Загружает страницу через Playwright с эмуляцией человека и повторными попытками"""
    async with async_playwright() as p:
        launch_args = [
            '--disable-blink-features=AutomationControlled',
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-web-security',
            '--disable-features=IsolateOrigins,site-per-process',
            '--window-size=1920,1080'
        ]
        
        browser = await p.chromium.launch(headless=True, args=launch_args)
        user_agent = await get_random_user_agent()
        context = await browser.new_context(
            user_agent=user_agent,
            viewport={'width': 1920, 'height': 1080},
            locale='ru-RU',
            timezone_id='Europe/Moscow'
        )
        
        page = await context.new_page()
        full_url = url + '?' + '&'.join([f"{k}={v}" for k, v in params.items()]) if params else url
        logger.info(f"Загрузка страницы: {full_url}")
        
        await page.goto(full_url, wait_until='domcontentloaded')
        await page.wait_for_timeout(random.randint(2000, 5000))
        
        # Скроллим как человек
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight/3)")
        await page.wait_for_timeout(random.randint(1000, 3000))
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight/2)")
        await page.wait_for_timeout(random.randint(1000, 3000))
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        
        # Ожидание появления карточек
        try:
            await page.wait_for_selector('article[data-name="CardComponent"]', timeout=15000)
        except:
            logger.warning("Селектор article[data-name] не найден, пробуем альтернативный")
            await page.wait_for_selector('div[data-testid="offer-card"]', timeout=15000)
        
        html = await page.content()
        await browser.close()
        return html

async def fetch_cian_deal_type(deal_type: str = 'sale') -> List[Ad]:
    """Собирает объявления с ЦИАН для указанного типа сделки"""
    params = {
        'deal_type': deal_type,
        'engine_version': '2',
        'offer_type': 'flat',
        'region': '1',
        'only_flat': '1',
        'sort': 'creation_date_desc',
        'p': '1'
    }
    # Добавляем все округа
    for d in DISTRICTS:
        code = {'ЦАО':8, 'САО':9, 'СВАО':10, 'ВАО':11, 'ЮВАО':12, 'ЮАО':13, 'ЮЗАО':14, 'ЗАО':15, 'СЗАО':16}.get(d)
        if code:
            params[f'okrug[{code}]'] = '1'
    
    url = "https://www.cian.ru/cat.php"
    html = await get_page_html_playwright(url, params)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'lxml')
    cards = soup.find_all('article', {'data-name': 'CardComponent'}) or \
            soup.find_all('div', {'data-testid': 'offer-card'}) or \
            soup.find_all('div', class_=re.compile('offer-card'))
    
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
            
            ad_id_match = re.search(r'/(\d+)/?$', link)
            ad_id = ad_id_match.group(1) if ad_id_match else hashlib.md5(link.encode()).hexdigest()
            if ad_id in seen_ids:
                continue
            seen_ids.add(ad_id)
            
            # Парсинг полей
            price_tag = (card.find('span', {'data-mark': 'MainPrice'}) or 
                        card.find('span', class_=re.compile('price')) or
                        card.find('meta', {'itemprop': 'price'}))
            if price_tag and price_tag.name == 'meta':
                price = price_tag.get('content', 'Цена не указана')
            else:
                price = price_tag.text.strip() if price_tag else 'Цена не указана'
            
            address_tag = (card.find('address') or 
                          card.find('span', class_=re.compile('address')) or
                          card.find('span', {'data-testid': 'address'}))
            address = address_tag.text.strip() if address_tag else 'Москва'
            
            metro_tag = (card.find('span', class_=re.compile('underground')) or 
                        card.find('a', href=re.compile('metro')))
            metro = metro_tag.text.strip() if metro_tag else 'Не указано'
            
            title_tag = card.find('h3') or card.find('a', {'data-testid': 'title'})
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
                area = f"{area_match.group(1).replace('.', ',')} м²"
            
            owner_tag = card.find('span', text=re.compile('собственник|без посредников', re.I))
            is_owner = bool(owner_tag)
            
            photos = []
            img_tags = card.find_all('img', src=True)[:10]
            for img in img_tags:
                src = img['src']
                if src.startswith('//'):
                    src = 'https:' + src
                if 'avatar' not in src and not src.endswith('.svg') and 'blank' not in src and 'placeholder' not in src:
                    photos.append(src)
            
            district_detected = None
            if DADATA_API_KEY:
                district_detected = await get_district_by_address(address)
            
            ad = Ad(
                id=ad_id,
                source='cian',
                deal_type=deal_type,
                title=title,
                link=link,
                price=price,
                address=address,
                metro=metro,
                floor=floor,
                area=area,
                rooms=rooms_count,
                owner=is_owner,
                photos=photos,
                district_detected=district_detected
            )
            results.append(ad)
        except Exception as e:
            logger.error(f"Ошибка парсинга карточки ЦИАН: {e}")
            continue
    
    logger.info(f"ЦИАН ({deal_type}): собрано {len(results)} объявлений")
    return results

async def fetch_avito_deal_type(deal_type: str = 'sale') -> List[Ad]:
    """Собирает объявления с Авито"""
    avito_category = {
        'sale': 'prodazha-kvartir',
        'rent': 'snyat-kvartiru'
    }.get(deal_type, 'prodazha-kvartir')
    
    url = f"https://www.avito.ru/moskva/kvartiry/{avito_category}"
    params = {
        's': '1',
        'p': '1'
    }
    
    html = await get_page_html_playwright(url, params, use_proxy=True)
    if not html:
        return []
    
    soup = BeautifulSoup(html, 'lxml')
    cards = soup.find_all('div', {'data-marker': 'item'}) or soup.find_all('div', {'itemtype': 'http://schema.org/Product'})
    
    results = []
    seen_ids = set()
    
    for card in cards[:30]:
        try:
            link_tag = card.find('a', {'data-marker': 'item-title'}) or card.find('a', href=True)
            if not link_tag:
                continue
            link = link_tag.get('href', '')
            if link.startswith('/'):
                link = 'https://www.avito.ru' + link
            
            ad_id_match = re.search(r'/(\d+)$', link)
            ad_id = ad_id_match.group(1) if ad_id_match else hashlib.md5(link.encode()).hexdigest()
            ad_id = f"avito_{ad_id}"
            
            if ad_id in seen_ids:
                continue
            seen_ids.add(ad_id)
            
            title_tag = card.find('meta', {'itemprop': 'name'})
            if title_tag:
                title = title_tag.get('content', '')
            else:
                title_tag = card.find('h3')
                title = title_tag.text.strip() if title_tag else 'Квартира'
            
            price_tag = card.find('meta', {'itemprop': 'price'})
            if price_tag:
                price = price_tag.get('content', 'Цена не указана') + ' ₽'
            else:
                price_tag = card.find('span', {'data-marker': 'item-price'})
                price = price_tag.text.strip() if price_tag else 'Цена не указана'
            
            address_tag = card.find('span', {'data-marker': 'item-address'})
            address = address_tag.text.strip() if address_tag else 'Москва'
            
            metro_tag = card.find('span', {'data-marker': 'item-metro'})
            metro = metro_tag.text.strip() if metro_tag else 'Не указано'
            if 'м.' in metro:
                metro = metro.replace('м.', '').strip()
            
            full_text = card.get_text(separator=' ', strip=True).lower()
            
            rooms_count = '?'
            room_match = re.search(r'(\d+)[-\s]комнат', full_text)
            if room_match:
                rooms_count = room_match.group(1)
            elif 'студия' in full_text:
                rooms_count = 'студия'
            
            floor = '?/?'
            floor_match = re.search(r'(\d+)[-\s]этаж\s+из\s+(\d+)', full_text)
            if floor_match:
                floor = f"{floor_match.group(1)}/{floor_match.group(2)}"
            
            area = '? м²'
            area_match = re.search(r'(\d+(?:[.,]\d+)?)\s*м²', full_text)
            if area_match:
                area = f"{area_match.group(1).replace('.', ',')} м²"
            
            is_owner = 'собственник' in full_text or 'без посредников' in full_text or 'частное лицо' in full_text
            
            photos = []
            img_tags = card.find_all('img', src=True)[:5]
            for img in img_tags:
                src = img['src']
                if src.startswith('//'):
                    src = 'https:' + src
                if 'avatar' not in src and not src.endswith('.svg'):
                    photos.append(src)
            
            district_detected = None
            if DADATA_API_KEY:
                district_detected = await get_district_by_address(address)
            
            ad = Ad(
                id=ad_id,
                source='avito',
                deal_type=deal_type,
                title=title,
                link=link,
                price=price,
                address=address,
                metro=metro,
                floor=floor,
                area=area,
                rooms=rooms_count,
                owner=is_owner,
                photos=photos,
                district_detected=district_detected
            )
            results.append(ad)
        except Exception as e:
            logger.error(f"Ошибка парсинга карточки Авито: {e}")
            continue
    
    logger.info(f"Авито ({deal_type}): собрано {len(results)} объявлений")
    return results

async def fetch_all_ads() -> List[Ad]:
    """Собирает объявления со всех источников"""
    tasks = [
        fetch_cian_deal_type('sale'),
        fetch_cian_deal_type('rent'),
        fetch_avito_deal_type('sale'),
        fetch_avito_deal_type('rent')
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    all_ads = []
    for res in results:
        if isinstance(res, Exception):
            logger.error(f"Ошибка в одном из парсеров: {res}")
        elif isinstance(res, list):
            all_ads.extend(res)
    
    seen = set()
    unique_ads = []
    for ad in all_ads:
        if ad.id not in seen:
            seen.add(ad.id)
            unique_ads.append(ad)
    
    logger.info(f"Всего собрано {len(unique_ads)} уникальных объявлений")
    return unique_ads

async def get_district_by_address(address: str) -> Optional[str]:
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
            async with sess.post(url, headers=headers, json=data, timeout=10) as resp:
                res = await resp.json()
        result = res[0]
        if result.get('area_type') == "округ" and result.get('area'):
            # Маппинг названий округов из DaData к вашим кодам (если нужно)
            return result['area']
    except Exception as e:
        logger.debug(f"Ошибка DaData: {e}")
    return None