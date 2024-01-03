# You will need to use the websocket-client libarty ... pip install websocket-client
import websocket
import base64
import logging
import json

# Enable debug output
websocket.enableTrace(False)

def on_message(ws, message):
    print(f"Received: {message}")

def on_error(ws, error):
    print(f"Error: {error}")

def on_close(ws, close_status_code, close_msg):
    print("### Closed ###")

def on_open(ws):
    print("### Opened ###")
    #payload = {"since": 0, "include_docs": True}
    payload = {"since": 0}
    ws.send(json.dumps(payload))

if __name__ == "__main__":

    username = "bob"
    password = "12345"
    hostname = "localhost"
    sgPort   = "4984"
    sgDbName = "sync_gateway"
    sgScope  = "fieldWork"
    sgCollection = "jobs"

    url = "ws://"+hostname+":"+sgPort+"/"+sgDbName+"."+sgScope+"."+sgCollection+"/_changes?feed=websocket"

    # Encode the username and password for the Authorization header
    auth_header = "Basic " + base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("utf-8")
    headers = ["Authorization: " + auth_header]

    # Create a WebSocketApp instance with the defined callbacks and headers
    ws_app = websocket.WebSocketApp(url,
                                    on_message=on_message,
                                    on_error=on_error,
                                    on_close=on_close,
                                    header=headers)

    # Assign the on_open callback
    ws_app.on_open = on_open

    # Run the WebSocketApp in a separate thread to keep the main thread free
    ws_app.run_forever()