import websockets
import asyncio
from json import dumps,loads
from dateutil.parser import parse
from math import ceil
from time import time

class Product:
	def __init__(self,product_id,time_to_run):
		self.time_to_run = time_to_run
		self.product = product_id
		self.current_price = 123
		self.match_book = []
		self.start_price = 0
		self.current_price = 0
		self.min_ask_price = 0
		self.max_bid_price = 0

		self.Start_Time = 0
		self.End_Time = 0

	async def on_order_message(self):
		async with websockets.connect("wss://ws-feed.gdax.com") as websocket_o:
			await websocket_o.send(dumps({"type": "subscribe", "product_ids": [self.product], "channels": ["level2"]}))

			text_file = open("order_book.txt", "w",1)
			# Initial Snapshot,build order book
			json_message = loads(await websocket_o.recv())
			text_file.write("%s\n" % (json_message))
			await websocket_o.recv()
			
			async for message in websocket_o:
				side, price, volume = loads(message)["changes"][0]
				price = float(price)
				volume = float(volume)
				localtime = time()
				text_file.write("%s\t%s\t%s\t%s\n" % (side,price,volume,localtime))


	async def on_match_message(self):
		async with websockets.connect("wss://ws-feed.gdax.com") as websocket_m:
			await websocket_m.send(dumps({"type":"subscribe",	"product_ids":[self.product], "channels":["matches"]}))
			# Skip the first 2 message
			await websocket_m.recv()
			await websocket_m.recv()
			match_book = open("match_book.txt", "w",1)

			async for message in websocket_m:
				message = loads(message)
				type = message["type"]
				side = message["side"]
				price = float(message["price"])
				size = float(message["size"])
				servertime = parse(message["time"]).timestamp()
				localtime = time()
				match_book.write("%s\t%s\t%s\t%s\t%s\t%s\n" % (type,side,price,size,servertime,localtime))
	def run(self,future):
		asyncio.ensure_future(self.on_match_message())
		asyncio.ensure_future(self.on_order_message())

class GDAX:
	def __init__(self, time_to_run):
		self.time_to_run = time_to_run
		self.products = {}
		self.products["BTC"] = Product("BTC-USD",self.time_to_run+2)
		# self.products["ETH"] = Product("ETH-USD",self.time_to_run+2)

	def start(self):
		loop = asyncio.get_event_loop()
		future = asyncio.Future()
		for key, product in self.products.items():
			product.run(future)
		loop.run_until_complete(future)
		# loop.close()

gdax = GDAX(2)
gdax.start()