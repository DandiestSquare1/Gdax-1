import gdax
import websockets
import asyncio
import threading
import time
import json
import math
import dateutil.parser
import numpy as np
from sortedcontainers import SortedDict
from datetime import datetime
from pprint import pprint

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
	global min_ask_price,max_bid_price,order_book_running
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
				order_book_running = False
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



async def compute_spread(unit):
	if unit <= 1:
		fluctuation = 0.005
	elif unit <= 30:
		fluctuation = 0.01
	elif unit <= 300:
		fluctuation = 0.05
	else:
		fluctuation = 0.1
	span = unit * 1000

	Interval_Start_Time = time.time()
	Next_Time = math.ceil(Interval_Start_Time) + unit/2
	await asyncio.sleep(Next_Time - Interval_Start_Time)

	while True:
		if not order_book_running:
			break
		for key in order_book.irange(max_bid_price * (1 - fluctuation), max_bid_price * (1 + fluctuation), (True,True)):
			print(key)
		print(time.time())

		Next_Time += unit
		await asyncio.sleep(Next_Time - time.time())


loop = asyncio.get_event_loop()
# loop.create_task(compute())
# loop.create_task(get_wss_order_book(5))
# loop.create_task(get_wss_match_book(5))


order_book_running = True
match_book_running = True
loop.run_until_complete(asyncio.wait({get_wss_match_book(2000)}))

def export_match_book(match_book,unit,interval):
	# Create a list of time intervals containing interval level info as a list: [Weighted_Price_Average,Volume,Interval_Start_Time,N_Transactions]
	# Only applicable when match book is not empty
	# length of result is interval
	# If an interval has 0 transaction then the interval list has 0 Volume and the price is the trailing price
	# Time is epoch time
	if len(match_book) > 0:
		
		start_price = match_book[0][0]
		Interval_Start_Time = math.ceil(match_book[0][2])
		text_file = open("Match_book_%s_by_%s.txt" % (interval, unit), "w")
		match_book_index = 1
		for i in range(interval):
			Amount = 0
			Weighted_Price_Average = start_price
			Volume = 0
			N_Transactions = 0
			Interval_Start_Time += unit

			while match_book_index < len(match_book) and match_book[match_book_index][2] < Interval_Start_Time:
				start_price = match_book[match_book_index][0]
				Volume += match_book[match_book_index][1]
				N_Transactions += 1
				Amount += match_book[match_book_index][0] * match_book[match_book_index][1]
				match_book_index += 1
			if Amount > 0:
				Volume = round(Volume,8)
				Weighted_Price_Average = round(Amount / Volume,2)


			text_file.write("%s,%s,%s,%s\n" % (Weighted_Price_Average, Volume, Interval_Start_Time, N_Transactions))
		text_file.close()
			

# print(order_book)
export_match_book(match_book,1,1800)

loop.close()