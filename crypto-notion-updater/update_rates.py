import os
import requests
import logging
from datetime import datetime
from time import sleep
from dotenv import load_dotenv

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

# Заголовки для Notion API
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

def query_notion_database(symbol):
    """Запрос к Notion API для поиска страницы по символу"""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    
    payload = {
        "filter": {
            "property": "Name",
            "title": {
                "equals": symbol
            }
        }
    }
    
    try:
        response = requests.post(url, json=payload, headers=NOTION_HEADERS, timeout=10)
        response.raise_for_status()
        return response.json().get("results", [])
    except Exception as e:
        logging.error(f"Error querying Notion for {symbol}: {e}")
        return []

def update_notion_page(page_id, price):
    """Обновление страницы в Notion"""
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
        return True
    except Exception as e:
        logging.error(f"Error updating page {page_id}: {e}")
        return False

def create_notion_page(symbol, coin_id, price):
    """Создание новой страницы в Notion"""
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
        return True
    except Exception as e:
        logging.error(f"Error creating page for {symbol}: {e}")
        return False

def update_notion_database():
    """Обновляем базу данных Notion"""
    try:
        # Получаем все цены одним запросом
        prices = get_all_prices()
        logging.info(f"Successfully fetched prices: {prices}")
        
        updated_count = 0
        created_count = 0
        
        for coin_id, symbol in CRYPTOS.items():
            try:
                if coin_id not in prices:
                    logging.error(f"No price data for {symbol} ({coin_id})")
                    continue
                
                current_price = prices[coin_id]
                
                # Ищем существующую запись
                results = query_notion_database(symbol)
                
                if results:
                    # Обновляем существующую запись
                    page_id = results[0]["id"]
                    if update_notion_page(page_id, current_price):
                        updated_count += 1
                        logging.info(f"Updated {symbol} price to {current_price}")
                    else:
                        logging.error(f"Failed to update {symbol}")
                else:
                    # Создаем новую запись
                    if create_notion_page(symbol, coin_id, current_price):
                        created_count += 1
                        logging.info(f"Created new entry for {symbol} with price {current_price}")
                    else:
                        logging.error(f"Failed to create {symbol}")
                        
            except Exception as e:
                logging.error(f"Error processing {symbol}: {str(e)}", exc_info=True)
        
        logging.info(f"Update completed: {updated_count} updated, {created_count} created")
                
    except Exception as e:
        logging.critical("Fatal error in Notion update", exc_info=True)
        raise

if __name__ == "__main__":
    update_notion_database()