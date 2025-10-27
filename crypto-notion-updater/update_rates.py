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
            
            logging.info(f"🔍 Processing {len(pages)} pages from database")
            
            # Обрабатываем каждую страницу
            for page in pages:
                try:
                    page_id = page.get("id")
                    properties = page.get("properties", {})
                    
                    # ДЕТАЛЬНОЕ ЛОГИРОВАНИЕ: выведем все свойства страницы
                    logging.info(f"📄 Page ID: {page_id}")
                    for prop_name, prop_value in properties.items():
                        logging.info(f"   Property '{prop_name}': {prop_value}")
                    
                    # Способ 1: Ищем название в свойстве Name (title)
                    name_property = properties.get("Name", {})
                    if name_property.get("title"):
                        title_list = name_property["title"]
                        if title_list and len(title_list) > 0:
                            page_name = title_list[0].get("text", {}).get("content", "").strip()
                            if page_name:
                                existing_pages[page_name.upper()] = page_id
                                logging.info(f"✅ Found by Name: '{page_name}' -> {page_id}")
                                continue
                    
                    # Способ 2: Ищем в свойстве Symbol (rich_text)
                    symbol_property = properties.get("Symbol", {})
                    if symbol_property.get("rich_text"):
                        rich_text_list = symbol_property["rich_text"]
                        if rich_text_list and len(rich_text_list) > 0:
                            symbol_name = rich_text_list[0].get("text", {}).get("content", "").strip()
                            if symbol_name:
                                existing_pages[symbol_name.upper()] = page_id
                                logging.info(f"✅ Found by Symbol: '{symbol_name}' -> {page_id}")
                                continue
                    
                    # Способ 3: Ищем в других текстовых свойствах
                    for prop_name, prop_value in properties.items():
                        if prop_value.get("rich_text"):
                            rich_text_list = prop_value["rich_text"]
                            if rich_text_list and len(rich_text_list) > 0:
                                text_content = rich_text_list[0].get("text", {}).get("content", "").strip().upper()
                                if text_content in [s.upper() for s in CRYPTOS.values()]:
                                    existing_pages[text_content] = page_id
                                    logging.info(f"✅ Found by property '{prop_name}': '{text_content}' -> {page_id}")
                                    break
                    
                    logging.warning(f"❌ Could not find identifiable name for page {page_id}")
                            
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
    
    logging.info(f"🎯 Total existing pages mapped: {len(existing_pages)}")
    logging.info(f"📋 Found symbols: {list(existing_pages.keys())}")
    return existing_pages

def update_notion_database():
    """Обновляем базу данных Notion"""
    try:
        # Инициализируем клиент
        notion = Client(auth=NOTION_TOKEN)
        
        # Получаем все существующие страницы ОДИН РАЗ
        existing_pages = get_existing_pages(notion)
        
        # Получаем все цены одним запросом
        prices = get_all_prices()
        logging.info(f"💰 Successfully fetched prices: {prices}")
        
        updated_count = 0
        created_count = 0
        error_count = 0
        
        for coin_id, symbol in CRYPTOS.items():
            try:
                if coin_id not in prices:
                    logging.error(f"❌ No price data for {symbol} ({coin_id})")
                    error_count += 1
                    continue
                
                current_price = prices[coin_id]
                symbol_upper = symbol.upper()
                
                logging.info(f"🔍 Looking for existing page: {symbol} ({symbol_upper})")
                
                # Ищем существующую страницу
                page_id = existing_pages.get(symbol_upper)
                
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
                        logging.info(f"✅ UPDATED {symbol} price to {current_price} (page: {page_id})")
                        
                    except Exception as update_error:
                        logging.error(f"❌ Failed to update {symbol}: {update_error}")
                        error_count += 1
                
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
                        logging.info(f"🆕 CREATED new entry for {symbol} with price {current_price}")
                    except Exception as create_error:
                        logging.error(f"❌ Failed to create {symbol}: {create_error}")
                        error_count += 1
                        
            except Exception as e:
                logging.error(f"❌ Error processing {symbol}: {str(e)}", exc_info=True)
                error_count += 1
        
        # Финальная статистика
        logging.info(f"🎯 UPDATE SUMMARY: {updated_count} updated, {created_count} created, {error_count} errors")
        
        # Если все записи создаются заново, выведем предупреждение
        if updated_count == 0 and created_count > 0:
            logging.warning("🚨 WARNING: All entries were created new! Existing pages were not found.")
            logging.warning("💡 Check if the property names in your Notion database match the code.")
                
    except Exception as e:
        logging.critical("💥 Fatal error in Notion update", exc_info=True)
        raise

if __name__ == "__main__":
    update_notion_database()