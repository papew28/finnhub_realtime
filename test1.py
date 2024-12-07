import websocket
import json
#

symbols = ["MSFT", "AAPL", "GOOGL", "AMZN"]
def on_message(ws, message):
    message=json.loads(list(message.split())[0])
    

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"MSFT"}')
   

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=coru6o9r01qturkgjqd0coru6o9r01qturkgjqdg",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()