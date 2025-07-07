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
NOTION_TOKEN = "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"
DATABASE_ID = "227c8f4b4994806ab6bfe68e33e914fa"
CRYPTOS = {
    "BTCUSDT": "BTC",
    "ETHUSDT": "ETH",
    "XRPUSDT": "XRP",
    "SOLUSDT": "SOL",
    "ADAUSDT": "ADA",
}

def get_binance_price(symbol, retries=3):
    """Получаем текущую цену с Binance с повторами при ошибках"""
    url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
    
    for attempt in range(retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return float(response.json()['price'])
            logging.warning(f"Attempt {attempt + 1}: Bad status code {response.status_code}")
        except Exception as e:
            logging.warning(f"Attempt {attempt + 1}: Error fetching price: {str(e)}")
        
        if attempt < retries - 1:
            sleep(2)
    
    raise Exception(f"Failed to get price after {retries} attempts")

def update_notion_database():
    """Обновляем базу данных Notion"""
    try:
        notion = Client(auth=NOTION_TOKEN)
        
        for symbol, name in CRYPTOS.items():
            try:
                current_price = get_binance_price(symbol)
                
                results = notion.databases.query(
                    database_id=DATABASE_ID,
                    filter={
                        "property": "Name",
                        "title": {"equals": name}
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
                    logging.info(f"Updated {name} price to {current_price}")
                else:
                    notion.pages.create(
                        parent={"database_id": DATABASE_ID},
                        properties={
                            "Name": {"title": [{"text": {"content": name}}]},
                            "Symbol": {"rich_text": [{"text": {"content": symbol}}]},
                            "Price": {"number": current_price},
                            "Last Updated": {"date": {"start": datetime.now().isoformat()}}
                        }
                    )
                    logging.info(f"Created new entry for {name} with price {current_price}")
                    
            except Exception as e:
                logging.error(f"Error processing {name}: {str(e)}", exc_info=True)
                
    except Exception as e:
        logging.critical("Fatal error in Notion update", exc_info=True)
        raise

if __name__ == "__main__":
    update_notion_database()