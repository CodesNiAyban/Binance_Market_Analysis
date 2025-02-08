import websocket
import json
import pandas as pd
import rel

# Defining the cryptocurrency pairs
crypto_pairs = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT']
crypto_pairs = [coin.lower() + "@kline_1m" for coin in crypto_pairs]

# Combining them into a single asset string
assets = '/'.join(crypto_pairs)

# Define how the data will be handled when a message is received
def on_message(ws, message):
    message = json.loads(message)
    manipulation(message)

# Manipulate and display the received data
def manipulation(source):
    # Extract the required price data from the WebSocket message
    rel_data = source['data']['k']['c']  # 'c' represents the close price
    event_time = pd.to_datetime(source['data']['E'], unit='ms')  # Convert timestamp to datetime
    
    # Assuming you want to store the data in a DataFrame (but 'c' is just a value)
    df = pd.DataFrame([[float(rel_data)]], columns=[source['data']['s']], index=[event_time])
    
    # Set index name for the dataframe
    df.index.name = 'timestamp'
    
    # Print the dataframe to verify the output
    print(df)

    return df

# WebSocket URL to subscribe to the data streams
socket = "wss://stream.binance.com:9443/stream?streams={}".format(assets)

# Start the WebSocket client
ws = websocket.WebSocketApp(socket, on_message=on_message)

# Run the WebSocket client indefinitely
ws.run_forever(dispatcher=rel, reconnect=5)  # Set dispatcher to automatic reconnection, 5 second reconnect delay if connection closed unexpectedly  
rel.signal(2, rel.abort)  # Keyboard Interrupt
rel.dispatch()  
