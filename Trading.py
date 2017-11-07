import gdax
import websockets
import asyncio
import threading
import time
import json
import math
import dateutil.parser
from collections import OrderedDict
from sortedcontainers import SortedDict
from datetime import datetime
from pprint import pprint

order_book = SortedDict()
match_book = []
interval_book = OrderedDict()
start_price = 0
current_price = 0
min_ask_price = 0
max_bid_price = 0

Start_Time = 0
End_Time = 0
order_book_running = True

async def get_wss_order_book(time_to_run):
	# Treat the order book function as the main thread, which controls start and end of other functions
	global min_ask_price,max_bid_price,current_price,order_book_running,order_book,Start_Time,End_Time

	async with websockets.connect("wss://ws-feed.gdax.com") as websocket_order_book:
		await websocket_order_book.send(json.dumps({"type": "subscribe", "product_ids": ["BTC-USD"], "channels": ["level2"]}))

		# Initial Snapshot,build order book
		json_message = json.loads(await websocket_order_book.recv())

		for [x,y] in json_message["asks"]:
			order_book[float(x)] = float(y)
		min_ask_price = order_book.peekitem(index = 0)[0]
		max_bid_price = min_ask_price - 0.01
		current_price = min_ask_price
		for [x,y] in json_message["bids"]:
			order_book[float(x)] = float(y)
		
		# Skip the next type:subscriptions message
		await websocket_order_book.recv()
		
		Start_Time = math.ceil(time.time())
		End_Time = Start_Time + time_to_run

		async for message in websocket_order_book:
			if time.time() > End_Time:
				order_book_running = False
				break
			side, price, Volume = json.loads(message)["changes"][0]
			price = float(price)
			Volume = float(Volume)
			
			if Volume == 0:
				del order_book[price]
			else:
				order_book[price] = Volume
				if side == "buy":
					max_bid_price = max(max_bid_price,price)
					min_ask_price = max_bid_price + 0.01
				else:
					min_ask_price = min(min_ask_price,price)
					max_bid_price = min_ask_price - 0.01

async def get_wss_match_book():
	global order_book_running,current_price

	async with websockets.connect("wss://ws-feed.gdax.com") as websocket_match_book:
		await websocket_match_book.send(json.dumps({"type":"subscribe",	"product_ids":["BTC-USD"], "channels":["matches"]}))
		# Skip the first 2 message
		await websocket_match_book.recv()
		await websocket_match_book.recv()

		async for message in websocket_match_book:
			if not order_book_running:
				break
			message = json.loads(message)
			current_price = float(message["price"])
			match_book.append((
				current_price,
				float(message["size"]),
				dateutil.parser.parse(message["time"]).timestamp()
				))

async def compute_spread(unit):
	# Expect the formula to produce the spread info to change in the future. Now it's simply giving center prices arbitrary more weight
	global order_book,order_book_running,Start_Time,End_Time,interval_book,current_price,start_price
	interval_book[unit] = OrderedDict()

	# Long interval has larger fluctuation
	if unit <= 1:
		fluctuation = 0.005
	elif unit <= 30:
		fluctuation = 0.01
	elif unit <= 300:
		fluctuation = 0.05
	else:
		fluctuation = 0.1


	# Sync with order book, compute spread at the mid moment in the interval
	while Start_Time == 0:
		await asyncio.sleep(0.1)

	Next_Time = Start_Time
	await asyncio.sleep(Next_Time + unit/2 - time.time())
	start_price = current_price

	while True:
		if not order_book_running or Next_Time + unit > End_Time:
			break
		Bid_Index = 0
		Ask_Index = 0

		# Giving price closer to the center more weight
		Adjusted_Volume = 0
		for price in order_book.irange(current_price * (1 - fluctuation), current_price, (True,True)):
			temp = (abs(price - current_price) / current_price / fluctuation + 0.1) * order_book[price]
			Adjusted_Volume += temp
			Bid_Index += temp * price
		Bid_Index = round(Bid_Index / Adjusted_Volume,2)
		
		Adjusted_Volume = 0
		for price in order_book.irange(current_price + 0.01, current_price * (1 + fluctuation), (True,True)):
			temp = (abs(price - current_price) / current_price / fluctuation + 0.1) * order_book[price]
			Adjusted_Volume += temp
			Ask_Index += temp * price
		Ask_Index = round(Ask_Index / Adjusted_Volume,2)
		interval_book[unit][int(Next_Time)] = (Bid_Index,Ask_Index)

		Next_Time += unit
		await asyncio.sleep(Next_Time + unit/2 - time.time())

loop = asyncio.get_event_loop()
# loop.create_task(get_wss_order_book(5))
# loop.create_task(get_wss_match_book(5))
loop.run_until_complete(asyncio.wait({get_wss_order_book(3600*9),get_wss_match_book(),compute_spread(1),compute_spread(30),compute_spread(120)}))
loop.close()

def export_interval_data():
	# Create lists of time intervals containing interval level info: (Time, trailing_price, Low, Weighted_Price_Average, High, close_price, N_Transactions, Volume, Bid_Index, Ask_Index, EMA_12, EMA_26, EMA_DIF, MACD, EMA_Up,EMA_Down,RSI, TSI, EMA_Volume, OBV_Delta)
	# If an interval has 0 transaction then the interval list has 0 Volume and the prices is the trailing price
	# Time is epoch time
	global match_book,interval_book,Start_Time,End_Time,start_price
	sign = lambda x: (x>0) - (x<0)

	for unit in interval_book.keys():
		text_file = open("Interval_Data_by_%s_Seconds.txt" % (unit), "w")
		text_file.write("Time\ttrailing_price\tLow\tWeighted_Price_Average\tHigh\tclose_price\tN_Transactions\tVolume\tBid_Index\tAsk_Index\tEMA_12\tEMA_26\tEMA_DIF\tMACD\tEMA_Up\tEMA_Down\tRSI\tTSI\tEMA_Volume\tOBV_Delta\n")
		trailing_price = start_price
		match_book_index = 0

		period = 0

		for Time,(Bid_Index,Ask_Index) in interval_book[unit].items():
			period += 1
			Amount = 0
			Volume = 0
			N_Transactions = 0
			Low = 999999
			High = 0

			while match_book_index < len(match_book) and match_book[match_book_index][2] < Time + unit:
				close_price = match_book[match_book_index][0]
				Low = min(Low,close_price)
				High = max(High,close_price)
				Volume += match_book[match_book_index][1]
				N_Transactions += 1
				Amount += match_book[match_book_index][0] * match_book[match_book_index][1]
				match_book_index += 1
			if Amount > 0:
				Volume = round(Volume,8)
				Weighted_Price_Average = round(Amount / Volume,2)
			else:
				Low = trailing_price
				High = trailing_price
				Weighted_Price_Average = trailing_price
				close_price = trailing_price

			if period == 1:
				EMA_12 = Weighted_Price_Average
				EMA_26 = Weighted_Price_Average
				EMA_DIF = 0
				EMA_Up = max(0,close_price - trailing_price)
				EMA_Down = max(0,trailing_price - close_price)
				EMA_Momentum = close_price - trailing_price
				EMA_Momentum_Abs = abs(close_price - trailing_price)
				EMA_Momentum_2 = EMA_Momentum
				EMA_Momentum_Abs_2 = EMA_Momentum_Abs
				EMA_Volume = Volume
				OBV_Delta = 0
			else:
				EMA_12 = round((1-2/(12+1)) * EMA_12 + 2/(12+1) * Weighted_Price_Average,6)
				EMA_26 = round((1-2/(26+1)) * EMA_26 + 2/(26+1) * Weighted_Price_Average,6)
				EMA_DIF = round((1-2/(9+1)) * EMA_DIF + 2/(9+1) * (EMA_12 - EMA_26),6)
				EMA_Up = round((1-1/14) * EMA_Up + 1/14 * max(0,close_price - trailing_price),6)
				EMA_Down = round((1-1/14) * EMA_Down + 1/14 * max(0,trailing_price - close_price),6)
				EMA_Momentum = (1-2/(25+1)) * EMA_Momentum + 2/(25+1) * (close_price - trailing_price)
				EMA_Momentum_Abs = (1-2/(25+1)) * EMA_Momentum_Abs + 2/(25+1) * abs(close_price - trailing_price)
				EMA_Momentum_2 = (1-2/(13+1)) * EMA_Momentum_2 + 2/(13+1) * EMA_Momentum
				EMA_Momentum_Abs_2 = (1-2/(13+1)) * EMA_Momentum_Abs_2 + 2/(13+1) * EMA_Momentum_Abs
				EMA_Volume = round((1-2/(12+1)) * EMA_Volume + 2/(12+1) * Volume,8)
				# OBV_Delta is generally used to confirm price moves. The idea is that volume is higher on days where the price move is in the dominant direction
				OBV_Delta = round(Volume * sign(close_price - trailing_price),8)

			MACD = round(EMA_12 - EMA_26 - EMA_DIF,6)
			RSI = 50 if EMA_Down == 0 else round((1-1/(1+(EMA_Up/EMA_Down))) * 100,2)
			TSI = 0 if EMA_Momentum_Abs_2 == 0 else round(EMA_Momentum_2 / EMA_Momentum_Abs_2 * 100,2)

			text_file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (Time, trailing_price, Low, Weighted_Price_Average, High, close_price, N_Transactions, Volume, Bid_Index, Ask_Index, EMA_12, EMA_26, EMA_DIF, MACD, EMA_Up,EMA_Down,RSI, TSI, EMA_Volume, OBV_Delta))
			trailing_price = close_price
		text_file.close()
			
export_interval_data()