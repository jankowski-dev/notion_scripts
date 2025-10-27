import os
import requests
import logging
from datetime import datetime
from time import sleep
from dotenv import load_dotenv
import concurrent.futures

# Настройка логгирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crypto_updater.log'),
        logging.StreamHandler()
    ]
)

# Загрузка переменных окружения
load_dotenv()

# Конфигурация
NOTION_TOKEN = os.getenv("NOTION_TOKEN")
DATABASE_ID = os.getenv("DATABASE_ID")
CRYPTOS = {
    "bitcoin": "BTC",
    "ethereum": "ETH", 
    "ripple": "XRP",
    "solana": "SOL",
    "cardano": "ADA",
    "tron": "TRX",
    "spark-2": "SPK",
    "tether": "USDT",
    "zora": "ZORA",
    "golem": "GLM",
    "vine": "VINE",
    "codatta": "codatta",
    "block-4": "block"
}

NOTION_HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}

def get_all_prices(retries=3):
    """Получаем все цены одним запросом"""
    coin_ids = ",".join(CRYPTOS.keys())
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_ids}&vs_currencies=usd"
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                data = response.json()
                return {coin_id: data[coin_id]['usd'] for coin_id in CRYPTOS if coin_id in data}
            
            logging.warning(f"Attempt {attempt + 1}: Status code {response.status_code}")
            if response.status_code == 429:
                reset_time = int(response.headers.get('Retry-After', 60))
                logging.info(f"Rate limit hit. Waiting {reset_time} seconds")
                sleep(reset_time)
                
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1}: Error - {str(e)}")
        
        if attempt < retries - 1:
            sleep(5)
    
    raise Exception(f"Failed to get prices after {retries} attempts")

def get_all_existing_pages():
    """Получаем ВСЕ страницы из базы ОДНИМ запросом"""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    all_pages = []
    start_cursor = None
    
    while True:
        payload = {"page_size": 100}
        if start_cursor:
            payload["start_cursor"] = start_cursor
            
        try:
            response = requests.post(url, json=payload, headers=NOTION_HEADERS, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            all_pages.extend(data.get("results", []))
            
            if data.get("has_more") and data.get("next_cursor"):
                start_cursor = data.get("next_cursor")
            else:
                break
                
        except Exception as e:
            logging.error(f"Error fetching pages: {e}")
            break
    
    # Создаем словарь для быстрого поиска: символ -> page_id
    page_map = {}
    for page in all_pages:
        properties = page.get("properties", {})
        name_prop = properties.get("Name", {})
        if name_prop.get("title"):
            name = name_prop["title"][0].get("text", {}).get("content", "").strip()
            if name:
                page_map[name] = page["id"]
    
    logging.info(f"Found {len(page_map)} existing pages")
    return page_map

def update_single_page(update_data):
    """Обновляет одну страницу"""
    page_id, price, symbol = update_data
    url = f"https://api.notion.com/v1/pages/{page_id}"
    
    payload = {
        "properties": {
            "Price": {"number": float(price)},
            "Last Updated": {"date": {"start": datetime.now().isoformat()}}
        }
    }
    
    try:
        response = requests.patch(url, json=payload, headers=NOTION_HEADERS, timeout=10)
        response.raise_for_status()
        return symbol, True, None
    except Exception as e:
        return symbol, False, str(e)

def create_single_page(create_data):
    """Создает одну страницу"""
    symbol, coin_id, price = create_data
    url = "https://api.notion.com/v1/pages"
    
    payload = {
        "parent": {"database_id": DATABASE_ID},
        "properties": {
            "Name": {"title": [{"text": {"content": symbol}}]},
            "Symbol": {"rich_text": [{"text": {"content": coin_id}}]},
            "Price": {"number": float(price)},
            "Last Updated": {"date": {"start": datetime.now().isoformat()}}
        }
    }
    
    try:
        response = requests.post(url, json=payload, headers=NOTION_HEADERS, timeout=10)
        response.raise_for_status()
        return symbol, True, None
    except Exception as e:
        return symbol, False, str(e)

def update_notion_database():
    """Обновляем базу данных Notion ОПТИМИЗИРОВАННО"""
    try:
        # Шаг 1: Получаем все цены ОДНИМ запросом
        prices = get_all_prices()
        logging.info(f"💰 Successfully fetched prices for {len(prices)} coins")
        
        # Шаг 2: Получаем все существующие страницы ОДНИМ запросом
        existing_pages = get_all_existing_pages()
        
        # Шаг 3: Разделяем на обновления и создания
        updates_to_do = []
        creations_to_do = []
        
        for coin_id, symbol in CRYPTOS.items():
            if coin_id not in prices:
                logging.error(f"No price data for {symbol} ({coin_id})")
                continue
            
            current_price = prices[coin_id]
            
            if symbol in existing_pages:
                # Добавляем в список для обновления
                updates_to_do.append((existing_pages[symbol], current_price, symbol))
            else:
                # Добавляем в список для создания
                creations_to_do.append((symbol, coin_id, current_price))
        
        logging.info(f"🔄 Planning: {len(updates_to_do)} updates, {len(creations_to_do)} creations")
        
        # Шаг 4: ВЫПОЛНЯЕМ ВСЕ ОБНОВЛЕНИЯ ПАРАЛЛЕЛЬНО
        updated_count = 0
        if updates_to_do:
            # Используем ThreadPool для параллельных запросов
            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(update_single_page, updates_to_do))
            
            for symbol, success, error in results:
                if success:
                    updated_count += 1
                    logging.info(f"✅ Updated {symbol}")
                else:
                    logging.error(f"❌ Failed to update {symbol}: {error}")
        
        # Шаг 5: ВЫПОЛНЯЕМ ВСЕ СОЗДАНИЯ ПАРАЛЛЕЛЬНО  
        created_count = 0
        if creations_to_do:
            with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
                results = list(executor.map(create_single_page, creations_to_do))
            
            for symbol, success, error in results:
                if success:
                    created_count += 1
                    logging.info(f"🆕 Created {symbol}")
                else:
                    logging.error(f"❌ Failed to create {symbol}: {error}")
        
        logging.info(f"🎯 COMPLETED: {updated_count} updated, {created_count} created")
                
    except Exception as e:
        logging.critical("💥 Fatal error in Notion update", exc_info=True)
        raise

if __name__ == "__main__":
    update_notion_database()