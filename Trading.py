import asyncio
import websockets
import threading
import gdax
import time
import json
from sortedcontainers import SortedDict
from pprint import pprint

public_client = gdax.PublicClient()
# a = public_client.get_product_order_book('BTC-USD', level = 2)
# a = public_client.get_product_ticker('BTC-USD')
# a = public_client.get_product_trades('BTC-USD')
# a = public_client.get_currencies()
# a = public_client.get_time()


# pprint(a)


order_book = SortedDict()
min_ask_price = 0
max_bid_price = 0

async def get_wss_feed():
	global min_ask_price,max_bid_price
	async with websockets.connect("wss://ws-feed.gdax.com") as websocket:
		message_data = {
		"type": "subscribe",
		"product_ids": ["BTC-USD"],
		"channels": ["level2"]}

		message_json = json.dumps(message_data)
		await websocket.send(message_json)

		# Initial Snapshot,build order book
		json_message = json.loads(await websocket.recv())

		for [x,y] in json_message["asks"]:
			order_book[float(x)] = y
		min_ask_price = order_book.peekitem(index = 0)[0]
		max_bid_price = min_ask_price - 0.01
		for [x,y] in json_message["bids"]:
			order_book[float(x)] = y
		
		await websocket.recv()
		
		async for message in websocket:
			price_type, price, size = json.loads(message)["changes"][0]
			price = float(price)
			size = float(size)			
			
			if size == 0:
				del order_book[price]
			else:
				order_book[price] = size
				if price_type == "buy":
					max_bid_price = max(max_bid_price,price)
					min_ask_price = max_bid_price + 0.01
				else:
					min_ask_price = min(min_ask_price,price)
					max_bid_price = min_ask_price - 0.01
			print(price,size)

async def compute():
	while True:
		sum = 0
		for _ in range(100):
			for key in order_book:
				sum += float(order_book[key])
		print(sum,"~~~~~~~~~")
		await asyncio.sleep(0.5)


loop = asyncio.get_event_loop()

loop.create_task(compute())
loop.create_task(get_wss_feed())


loop.run_forever()
loop.close()