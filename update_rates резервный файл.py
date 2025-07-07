import os
import requests
import logging
from time import sleep
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

def get_crypto_price(coin_id, retries=3):
    """Получаем текущую цену с CoinGecko API"""
    url = f"https://api.coingecko.com/api/v3/simple/price?ids={coin_id}&vs_currencies=usd"
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if coin_id in data and 'usd' in data[coin_id]:
                    return float(data[coin_id]['usd'])
                raise ValueError(f"No price data for {coin_id}")
            logging.warning(f"Attempt {attempt + 1}: Status code {response.status_code}")
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1}: Error fetching price - {str(e)}")
        
        if attempt < retries - 1:
            sleep(2)
    
    raise Exception(f"Failed to get price after {retries} attempts")

def update_notion_database():
    """Обновляем базу данных Notion"""
    try:
        notion = Client(auth=NOTION_TOKEN)
        
        for coin_id, symbol in CRYPTOS.items():
            try:
                current_price = get_crypto_price(coin_id)
                
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