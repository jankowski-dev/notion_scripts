import os
import requests
import logging
from datetime import datetime
from time import sleep
from notion_client import Client
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

def get_existing_pages(notion):
    """Получаем все существующие страницы и создаем словарь для поиска"""
    existing_pages = {}
    start_cursor = None
    
    try:
        while True:
            # Получаем страницы пачками
            query_params = {
                "database_id": DATABASE_ID,
                "page_size": 100
            }
            if start_cursor:
                query_params["start_cursor"] = start_cursor
                
            response = notion.databases.query(**query_params)
            pages = response.get("results", [])
            
            # Обрабатываем каждую страницу
            for page in pages:
                try:
                    # Получаем название из свойства Name
                    name_property = page.get("properties", {}).get("Name", {})
                    if name_property.get("title"):
                        page_name = name_property["title"][0].get("text", {}).get("content", "").strip()
                        if page_name:
                            existing_pages[page_name.upper()] = page["id"]
                            logging.debug(f"Found existing page: {page_name} -> {page['id']}")
                    
                    # Дополнительно ищем по свойству Symbol (на случай если поиск по Name не работает)
                    symbol_property = page.get("properties", {}).get("Symbol", {})
                    if symbol_property.get("rich_text"):
                        symbol_name = symbol_property["rich_text"][0].get("text", {}).get("content", "").strip()
                        if symbol_name and symbol_name.upper() not in existing_pages:
                            existing_pages[symbol_name.upper()] = page["id"]
                            logging.debug(f"Found existing page by symbol: {symbol_name} -> {page['id']}")
                            
                except Exception as e:
                    logging.warning(f"Error processing page {page.get('id')}: {e}")
                    continue
            
            # Проверяем есть ли еще страницы
            if response.get("has_more") and response.get("next_cursor"):
                start_cursor = response.get("next_cursor")
            else:
                break
                
    except Exception as e:
        logging.error(f"Error fetching existing pages: {e}")
    
    logging.info(f"Found {len(existing_pages)} existing pages in database")
    return existing_pages

def update_notion_database():
    """Обновляем базу данных Notion"""
    try:
        # Инициализируем клиент
        notion = Client(auth=NOTION_TOKEN)
        
        # Получаем все существующие страницы ОДИН РАЗ
        existing_pages = get_existing_pages(notion)
        logging.info(f"Existing pages mapped: {list(existing_pages.keys())}")
        
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
                symbol_upper = symbol.upper()
                
                # Ищем существующую страницу по разным ключам
                page_id = None
                
                # Пробуем найти по символу (BTC, ETH и т.д.)
                if symbol_upper in existing_pages:
                    page_id = existing_pages[symbol_upper]
                    logging.debug(f"Found {symbol} by symbol: {page_id}")
                
                # Если не нашли, пробуем найти по полному имени крипты
                if not page_id and coin_id.upper() in existing_pages:
                    page_id = existing_pages[coin_id.upper()]
                    logging.debug(f"Found {symbol} by coin_id: {page_id}")
                
                if page_id:
                    # ОБНОВЛЯЕМ существующую запись
                    try:
                        notion.pages.update(
                            **{
                                "page_id": page_id,
                                "properties": {
                                    "Price": {"number": float(current_price)},
                                    "Last Updated": {"date": {"start": datetime.now().isoformat()}}
                                }
                            }
                        )
                        updated_count += 1
                        logging.info(f"✅ Updated {symbol} price to {current_price}")
                        
                    except Exception as update_error:
                        logging.error(f"Failed to update {symbol}: {update_error}")
                        # Пробуем создать новую запись если обновление не удалось
                        try:
                            notion.pages.create(
                                **{
                                    "parent": {"database_id": DATABASE_ID},
                                    "properties": {
                                        "Name": {"title": [{"text": {"content": symbol}}]},
                                        "Symbol": {"rich_text": [{"text": {"content": coin_id}}]},
                                        "Price": {"number": float(current_price)},
                                        "Last Updated": {"date": {"start": datetime.now().isoformat()}}
                                    }
                                }
                            )
                            created_count += 1
                            logging.info(f"🆕 Created new entry for {symbol} with price {current_price} (update failed)")
                        except Exception as create_error:
                            logging.error(f"Failed to create {symbol}: {create_error}")
                
                else:
                    # СОЗДАЕМ новую запись
                    try:
                        notion.pages.create(
                            **{
                                "parent": {"database_id": DATABASE_ID},
                                "properties": {
                                    "Name": {"title": [{"text": {"content": symbol}}]},
                                    "Symbol": {"rich_text": [{"text": {"content": coin_id}}]},
                                    "Price": {"number": float(current_price)},
                                    "Last Updated": {"date": {"start": datetime.now().isoformat()}}
                                }
                            }
                        )
                        created_count += 1
                        logging.info(f"🆕 Created new entry for {symbol} with price {current_price}")
                    except Exception as create_error:
                        logging.error(f"Failed to create {symbol}: {create_error}")
                        
            except Exception as e:
                logging.error(f"Error processing {symbol}: {str(e)}", exc_info=True)
        
        # Финальная статистика
        logging.info(f"🎯 Update completed: {updated_count} updated, {created_count} created, {len(CRYPTOS) - updated_count - created_count} failed")
                
    except Exception as e:
        logging.critical("Fatal error in Notion update", exc_info=True)
        raise

if __name__ == "__main__":
    update_notion_database()