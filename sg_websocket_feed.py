import websocket
import base64
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
    payload = {"since": 0, "include_docs": True}
    #payload["active_only"] = True #the feed will NOT send `_deleted` docs
    
    ### FILTER BY CHANNELS ###
    #channels = ["bob", "water", "cake"]
    #payload["filter"] = "sync_gateway/bychannel"
    #payload["channels"] = channels
        
    ws.send(json.dumps(payload))

if __name__ == "__main__":

    secure = False  # Set to True for wss (required for Couchbase Capella)
    host = "localhost"
    port   = "4984"
    sgDb = "db"
    sgScope  = "us"
    sgCollection = "prices"
    username = "bob"
    password = "password"

    protocol = 'wss' if secure else 'ws'
    url = protocol+"://"+host+":"+port+"/"+sgDb+"."+sgScope+"."+sgCollection+"/_changes?feed=websocket"

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