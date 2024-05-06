import websocket
import finnhub
import avro.schema
from avro.io import DatumWriter, DatumReader
import io
from .constant import api_key
import json

def connect_client():
       return finnhub.Client(api_key=api_key)

def validate_ticker(ticker):
    try:
        finnhub_client = connect_client()
        results=finnhub_client.symbol_lookup(ticker)["result"]
        for stock in results:
            if stock["symbol"]==ticker:
                return True
        return False
    except Exception as e:
        print(e)
        return None
    

def encode_avro(schema, data):
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(data, encoder)
    raw_bytes = bytes_writer.getvalue()
    return raw_bytes
