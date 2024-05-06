from confluent_kafka import  SerializingProducer
import websocket
import os
import sys
import json

sys.path.insert(0,os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.functions import validate_ticker,encode_avro
from utils.constant import avro_schema,ticker,api_key


producer_conf={
    'bootstrap.servers': 'localhost:9092'
}

producer=SerializingProducer(producer_conf)

topic_name='trading_data'

def on_message(ws, message):
    message=json.loads(message)
    avro_mess=encode_avro(avro_schema,{
                            "data":message["data"],
                            "type":message["type"]
                          } 
                       )
    print(message["data"])
    #producer.send(topic_name,value=avro_mess)
    
def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    #validate the ticker
    #for tiker in ticker:
        if validate_ticker(ticker):
            #subsribe to the ticker
            ws.send(f'{{"type":"subscribe","symbol":"{ticker}"}}')
        else:
            print(f"{ticker} is not a valid ticker")

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(f"wss://ws.finnhub.io?token={api_key}",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()






