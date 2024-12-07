from confluent_kafka import SerializingProducer
import websocket
import os
import sys
import json
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.functions import validate_ticker, encode_avro
from utils.constant import avro_schema, api_key

symbols = ["MSFT", "AAPL", "GOOGL", "AMZN", "BTC-USD"]  

producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'api.version.request': False,
    'broker.version.fallback': '0.9.0',
    'client.id': 'finnhub-market-data',
    'retries': 3,
    'retry.backoff.ms': 1000,
    'batch.size': 16384,
    'linger.ms': 50,
    'error_cb': lambda err: print(f"Kafka error: {err}")
}

producer = SerializingProducer(producer_conf)
topic_name = 'trading_data'  

def on_message(ws, message):
    try:
        message = json.loads(message)
        avro_mess = encode_avro(avro_schema, {
            "data": message["data"],
            "type": message["type"]
        })
        producer.produce(topic_name, value=avro_mess)
        producer.poll(0)
        
        if "data" in message and len(message["data"]) > 0:
            print(f"Processed data for {message['data'][0]['s']}")
            
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print(f"WebSocket error: {error}")

def on_close(ws):
    print("### closed ###")
    producer.flush()

def on_open(ws):
    for symbol in symbols:
        if validate_ticker(symbol):
            subscribe_message = {
                "type": "subscribe",
                "symbol": symbol
            }
            ws.send(json.dumps(subscribe_message))
            print(f"Subscribed to {symbol}")
        else:
            print(f"{symbol} is not a valid ticker")

if __name__ == "__main__":
    websocket.enableTrace(True)
    
    while True:
        try:
            ws = websocket.WebSocketApp(
                f"wss://ws.finnhub.io?token={api_key}",
                on_message=on_message,
                on_error=on_error,
                on_close=on_close
            )
            ws.on_open = on_open
            ws.run_forever()
        except Exception as e:
            print(f"Connection error: {e}")
            time.sleep(5)  