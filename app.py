import time
from binance.client import Client
import pymysql.cursors
import pandas as pd
import threading
import mysql.connector


client = Client()

# List of coins to fetch data for
coins = ["sxp", "chess", "blz", "joe", "perl", "ach", "gmt", "xrp", "akro", "zil"]

def get_db_connection():
    connection = pymysql.connect(
        host='localhost',
        user='root',
        password='Ran@123456',
        db='ransql',
        charset='utf8',
        cursorclass=pymysql.cursors.DictCursor,
    )
    return connection

# Create tables for each coin
def create_tables():
    connection = get_db_connection()
    cursor = connection.cursor()
    for coin in coins:
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {coin}usdt (
                id INT AUTO_INCREMENT PRIMARY KEY,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                price DECIMAL(18, 8),
                volume DECIMAL(18, 8)
            )
        """)
    connection.commit()
    connection.close()
def fetch_and_store_recent_trades(coin):
    last_trade_timestamp = None
    while True:
        try:
            connection = get_db_connection()
            cursor = connection.cursor()
            trades = client.get_recent_trades(symbol=f'{coin.upper()}USDT')
            for trade in trades:
                trade_timestamp = pd.to_datetime(trade['time'], unit='ms')
                if last_trade_timestamp is None or trade_timestamp > last_trade_timestamp:
                    cursor.execute(f"INSERT INTO {coin}usdt (timestamp, price, volume) VALUES (%s, %s, %s)", (trade_timestamp, trade['price'], trade['qty']))
                    last_trade_timestamp = trade_timestamp
            connection.commit()
        except Exception as e:
            print(f'Error fetching and storing trades for {coin}: {e}')
            connection.rollback()
            # Wait for 30 seconds before retrying
            time.sleep(30)
            continue
        finally:
            connection.close()
        time.sleep(5)


# Function to delete data older than 1 hour
def delete_old_data(coin):
    while True:
        connection = get_db_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(f"DELETE FROM {coin}usdt WHERE timestamp < NOW() - INTERVAL 1 HOUR")
            connection.commit()
        except Exception as e:
            print(f'Error deleting old data for {coin}: {e}')
            connection.rollback()
        finally:
            connection.close()
        time.sleep(3000)  # Sleep for 50 minutes

# Start the threads
def start_threads():
    create_tables()
    for coin in coins:
        threading.Thread(target=fetch_and_store_recent_trades, args=(coin,)).start()
        threading.Thread(target=delete_old_data, args=(coin,)).start()

start_threads()

while True:
    time.sleep(2)
