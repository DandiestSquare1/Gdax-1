import asyncio
import websockets
import threading
import gdax
import time
from datetime import datetime
import json
from sortedcontainers import SortedDict
from pprint import pprint
import math
import dateutil.parser


public_client = gdax.PublicClient()
# a = public_client.get_product_order_book('BTC-USD', level = 2)
# a = public_client.get_product_ticker('BTC-USD')
# a = public_client.get_product_trades('BTC-USD')
# a = public_client.get_currencies()
# a = public_client.get_time()


# pprint(a)


order_book = SortedDict()
match_book = []
min_ask_price = 0
max_bid_price = 0

async def get_wss_match_book(time_to_run):
	async with websockets.connect("wss://ws-feed.gdax.com") as websocket_match_book:
		await websocket_match_book.send(json.dumps({"type":"subscribe",	"product_ids":["BTC-USD"], "channels":["matches"]}))
		# Skip the first 2 message
		await websocket_match_book.recv()
		await websocket_match_book.recv()

		end_time = time.time() + time_to_run
		async for message in websocket_match_book:
			if time.time() > end_time:
				break
			message = json.loads(message)
			match_book.append((
				float(message["price"]),
				float(message["size"]),
				dateutil.parser.parse(message["time"]).timestamp()
				))


async def get_wss_order_book(time_to_run):
	global min_ask_price,max_bid_price
	async with websockets.connect("wss://ws-feed.gdax.com") as websocket_order_book:
		await websocket_order_book.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["level2"]}))

		# Initial Snapshot,build order book
		json_message = json.loads(await websocket_order_book.recv())

		for [x,y] in json_message["asks"]:
			order_book[float(x)] = y
		min_ask_price = order_book.peekitem(index = 0)[0]
		max_bid_price = min_ask_price - 0.01
		for [x,y] in json_message["bids"]:
			order_book[float(x)] = y
		
		# Skip the next type:subscriptions message
		await websocket_order_book.recv()
		
		end_time = time.time() + time_to_run
		async for message in websocket_order_book:
			if time.time() > end_time:
				break
			side, price, size = json.loads(message)["changes"][0]
			price = float(price)
			size = float(size)			
			
			if size == 0:
				del order_book[price]
			else:
				order_book[price] = size
				if side == "buy":
					max_bid_price = max(max_bid_price,price)
					min_ask_price = max_bid_price + 0.01
				else:
					min_ask_price = min(min_ask_price,price)
					max_bid_price = min_ask_price - 0.01
			# print(price,size)

async def compute():
	while True:
		sum = 0
		for _ in range(100):
			for key in order_book:
				sum += float(order_book[key])
		print(sum,"~~~~~~~~~")
		await asyncio.sleep(0.5)


loop = asyncio.get_event_loop()
# loop.create_task(compute())
# loop.create_task(get_wss_order_book(5))
# loop.create_task(get_wss_match_book(5))
loop.run_until_complete(asyncio.wait({get_wss_order_book(3),get_wss_match_book(60)}))

def export_match_book(match_book,unit,interval):
	# Create a list of time intervals containing interval level info as a list: [Weighted_Average_Price,Volumn,Interval_Start_Time,N_Transactions]
	# Only applicable when match book is not empty
	# length of result is interval
	# If an interval has 0 transaction then the interval list has 0 volumn and the price is the trailing price
	# Time is epoch time
	result = []
	if len(match_book) > 0:
		start_price = match_book[0][0]
		match_book_index = 1
		for i, start_time in enumerate(range(math.ceil(match_book[0][2]),math.ceil(match_book[0][2]) + unit * interval,unit)):
			result.append([start_price,0,start_time,0])
			amount = 0
			while match_book_index < len(match_book) and match_book[match_book_index][2] < start_time + 1:
				start_price = match_book[match_book_index][0]
				result[i][1] += match_book[match_book_index][1]
				result[i][3] += 1
				amount += match_book[match_book_index][0] * match_book[match_book_index][1]
				match_book_index += 1
			if amount > 0:
				result[i][0] = round(amount / result[i][1],2)
				result[i][1] = round(result[i][1],8)
	return result
			

print(match_book)
pprint(export_match_book(match_book,1,60))

loop.close()