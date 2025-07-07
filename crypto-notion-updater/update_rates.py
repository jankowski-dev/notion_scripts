import os
import requests
import logging
from datetime import datetime
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
}

def get_all_prices(retries=3):
    """Получаем все цены одним запросом"""
    coin_ids = ",".join(CRYPTOS.keys())
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_ids}&vs_currencies=usd"
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=15)
            
            if response.status_code == 200:
                data = response.json()
                return {coin_id: float(data[coin_id]['usd']) for coin_id in CRYPTOS if coin_id in data}
            
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

def update_notion_batch(prices):
    """Обновляем Notion одним batch-запросом"""
    try:
        notion = Client(auth=NOTION_TOKEN)
        now = datetime.now().isoformat()
        
        # 1. Получаем все текущие записи
        response = notion.databases.query(
            database_id=DATABASE_ID,
            filter={
                "or": [
                    {"property": "Name", "title": {"equals": symbol}}
                    for symbol in CRYPTOS.values()
                ]
            }
        )
        
        existing_pages = {
            page["properties"]["Name"]["title"][0]["plain_text"]: page["id"]
            for page in response.get("results", [])
        }
        
        # 2. Подготавливаем batch-операции
        operations = []
        
        for coin_id, symbol in CRYPTOS.items():
            if coin_id not in prices:
                logging.error(f"No price data for {symbol}")
                continue
                
            price = prices[coin_id]
            properties = {
                "Price": {"number": price},
                "Last Updated": {"date": {"start": now}},
                "Symbol": {"rich_text": [{"text": {"content": coin_id}}]}
            }
            
            if symbol in existing_pages:
                # Операция обновления
                operations.append({
                    "update": {
                        "page_id": existing_pages[symbol],
                        "properties": properties
                    }
                })
            else:
                # Операция создания
                properties["Name"] = {"title": [{"text": {"content": symbol}}]}
                operations.append({
                    "create": {
                        "parent": {"database_id": DATABASE_ID},
                        "properties": properties
                    }
                })
        
        # 3. Выполняем batch-запрос
        if operations:
            notion.request(
                path="batch",
                method="PATCH",
                body={"operations": operations}
            )
            logging.info(f"Successfully updated {len(operations)} records in Notion")
        
    except Exception as e:
        logging.critical("Fatal error in batch update", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        prices = get_all_prices()
        logging.info(f"Fetched prices: {prices}")
        update_notion_batch(prices)
    except Exception as e:
        logging.error(f"Script failed: {str(e)}", exc_info=True)