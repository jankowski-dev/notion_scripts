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
    "spark-2": "SPK",
    "tether": "USDT",
    "zora": "ZORA",
    "golem": "GLM",
    "vine": "VINE"
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

def update_notion_database():
    """Обновляем базу данных Notion"""
    try:
        notion = Client(auth=NOTION_TOKEN)
        
        # Получаем все цены одним запросом
        prices = get_all_prices()
        logging.info(f"Successfully fetched prices: {prices}")
        
        for coin_id, symbol in CRYPTOS.items():
            try:
                if coin_id not in prices:
                    logging.error(f"No price data for {symbol} ({coin_id})")
                    continue
                
                current_price = prices[coin_id]
                
                results = notion.databases.query(
                    database_id=DATABASE_ID,
                    filter={
                        "property": "Name",
                        "title": {"equals": symbol}
                    }
                ).get("results")
                
                if results:
                    notion.pages.update(
                        page_id=results[0]["id"],
                        properties={
                            "Price": {"number": current_price},
                            "Last Updated": {"date": {"start": datetime.now().isoformat()}}
                        }
                    )
                    logging.info(f"Updated {symbol} price to {current_price}")
                else:
                    notion.pages.create(
                        parent={"database_id": DATABASE_ID},
                        properties={
                            "Name": {"title": [{"text": {"content": symbol}}]},
                            "Symbol": {"rich_text": [{"text": {"content": coin_id}}]},
                            "Price": {"number": current_price},
                            "Last Updated": {"date": {"start": datetime.now().isoformat()}}
                        }
                    )
                    logging.info(f"Created new entry for {symbol} with price {current_price}")
                    
            except Exception as e:
                logging.error(f"Error processing {symbol}: {str(e)}", exc_info=True)
                
    except Exception as e:
        logging.critical("Fatal error in Notion update", exc_info=True)
        raise

if __name__ == "__main__":
    update_notion_database()