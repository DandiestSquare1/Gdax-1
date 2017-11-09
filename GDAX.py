import websockets
import asyncio
from json import dumps,loads
from dateutil.parser import parse
from math import ceil
from time import time
from collections import OrderedDict
from sortedcontainers import SortedDict


class Product:
	def __init__(self,product_id,time_to_run):
		self.time_to_run = time_to_run
		self.product = product_id
		self.current_price = 123
		self.match_book = []
		self.order_book = SortedDict()


		self.interval_book = OrderedDict()
		self.start_price = 0
		self.current_price = 0
		self.min_ask_price = 0
		self.max_bid_price = 0

		self.Start_Time = 0
		self.End_Time = 0

	async def stop_future(self,future):
		await asyncio.sleep(self.time_to_run)
		future.set_result("Done")
		future.cancel()

	async def on_order_message(self):
		async with websockets.connect("wss://ws-feed.gdax.com") as websocket_o:
			await websocket_o.send(dumps({"type": "subscribe", "product_ids": [self.product], "channels": ["level2"]}))

			# Initial Snapshot,build order book
			json_message = loads(await websocket_o.recv())

			for [x,y] in json_message["asks"]:
				self.order_book[float(x)] = float(y)
			self.min_ask_price = self.order_book.peekitem(index = 0)[0]
			self.max_bid_price = self.min_ask_price - 0.01
			self.current_price = self.min_ask_price
			self.start_price = self.current_price
			for [x,y] in json_message["bids"]:
				self.order_book[float(x)] = float(y)
			
			# Skip the next type:subscriptions message
			await websocket_o.recv()
			
			self.Start_Time = ceil(time())
			self.End_Time = self.Start_Time + self.time_to_run

			async for message in websocket_o:
				side, price, volume = loads(message)["changes"][0]
				price = float(price)
				volume = float(volume)

				self.repost()

				if volume == 0:
					del self.order_book[price]
				else:
					self.order_book[price] = volume
					if side == "buy":
						self.max_bid_price = max(self.max_bid_price,price)
						self.min_ask_price = self.max_bid_price + 0.01
					else:
						self.min_ask_price = min(self.min_ask_price,price)
						self.max_bid_price = self.min_ask_price - 0.01

	async def on_match_message(self):
		async with websockets.connect("wss://ws-feed.gdax.com") as websocket_m:
			await websocket_m.send(dumps({"type":"subscribe",	"product_ids":[self.product], "channels":["matches"]}))
			# Skip the first 2 message
			await websocket_m.recv()
			await websocket_m.recv()

			async for message in websocket_m:
				message = loads(message)

				# Update Current Price
				self.current_price = float(message["price"])

				# Update Match Book
				self.match_book.append((
					self.current_price,
					float(message["size"]),
					parse(message["time"]).timestamp()
					))

	async def on_interval(self,interval):
		# Expect the formula to produce the spread info to change in the future. Now it's simply giving center prices arbitrary more weight
		self.interval_book[interval] = OrderedDict()
		sign = lambda x: (x>0) - (x<0)
		# Long interval has larger fluctuation
		if interval <= 1:
			fluctuation = 0.005
		elif interval <= 30:
			fluctuation = 0.01
		elif interval <= 300:
			fluctuation = 0.05
		else:
			fluctuation = 0.1

		# text_file = open("Interval_Data_by_%s_Seconds.txt" % (interval), "w")
		# text_file.write("Time\ttrailing_price\tLow\tWeighted_Price_Average\tHigh\tclose_price\tN_Transactions\tVolume\tBid_Index\tAsk_Index\tEMA_12\tEMA_26\tEMA_DIF\tMACD\tEMA_Up\tEMA_Down\tRSI\tTSI\tEMA_Volume\tOBV_Delta\n")

		# Sync with order book, compute spread at the mid moment in the interval
		while self.Start_Time == 0:
			await asyncio.sleep(0.1)

		This_Moment = self.Start_Time
		await asyncio.sleep(This_Moment + interval - time())

		match_book_index = 0
		trailing_price = self.start_price
		while match_book_index < len(self.match_book) and self.match_book[match_book_index][2] < This_Moment:
			trailing_price = self.match_book[match_book_index][0]
			match_book_index += 1


		while True:
			Bid_Index = 0
			Ask_Index = 0
			# Giving price closer to the center more weight
			Adjusted_Volume = 0
			for price in self.order_book.irange(self.current_price * (1 - fluctuation), self.current_price, (True,True)):
				temp = (abs(price - self.current_price) / self.current_price / fluctuation + 0.1) * self.order_book[price]
				Adjusted_Volume += temp
				Bid_Index += temp * price
			Bid_Index = round(Bid_Index / Adjusted_Volume,2)
			
			Adjusted_Volume = 0
			for price in self.order_book.irange(self.current_price + 0.01, self.current_price * (1 + fluctuation), (True,True)):
				temp = (abs(price - self.current_price) / self.current_price / fluctuation + 0.1) * self.order_book[price]
				Adjusted_Volume += temp
				Ask_Index += temp * price
			Ask_Index = round(Ask_Index / Adjusted_Volume,2)


			Amount = 0
			Volume = 0
			N_Transactions = 0
			Low = 999999
			High = 0
			while match_book_index < len(self.match_book) and self.match_book[match_book_index][2] < This_Moment + interval:
				close_price = self.match_book[match_book_index][0]
				Low = min(Low,close_price)
				High = max(High,close_price)
				Volume += self.match_book[match_book_index][1]
				N_Transactions += 1
				Amount += self.match_book[match_book_index][0] * self.match_book[match_book_index][1]
				match_book_index += 1
			if Amount > 0:
				Volume = round(Volume,8)
				Weighted_Price_Average = round(Amount / Volume,2)
			else:
				Low = trailing_price
				High = trailing_price
				Weighted_Price_Average = trailing_price
				close_price = trailing_price

			if This_Moment == self.Start_Time:
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
			
			self.interval_book[interval][This_Moment] = (This_Moment, trailing_price, Low, Weighted_Price_Average, High, close_price, N_Transactions, Volume, Bid_Index, Ask_Index, EMA_12, EMA_26, EMA_DIF, MACD, EMA_Up,EMA_Down,RSI, TSI, EMA_Volume, OBV_Delta)
			# text_file.write(*self.interval_book[interval][This_Moment],sep = '\t')
			print(self.interval_book[interval][This_Moment])
			This_Moment += interval
			trailing_price = close_price
			await asyncio.sleep(This_Moment + interval - time())

			# text_file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (This_Moment, trailing_price, Low, Weighted_Price_Average, High, close_price, N_Transactions, Volume, Bid_Index, Ask_Index, EMA_12, EMA_26, EMA_DIF, MACD, EMA_Up,EMA_Down,RSI, TSI, EMA_Volume, OBV_Delta))

		text_file.close()

	def repost(self):
		return
	def market_buy_sell(self):
		return	
	def limit_buy_sell(self):
		return

	def run(self,future):
		asyncio.ensure_future(self.on_match_message())
		asyncio.ensure_future(self.on_order_message())
		asyncio.ensure_future(self.on_interval(1))
		asyncio.ensure_future(self.on_interval(5))
		asyncio.ensure_future(self.stop_future(future))

class GDAX:
	def __init__(self, time_to_run):
		self.time_to_run = time_to_run
		self.products = {}
		self.products["BTC"] = Product("BTC-USD",self.time_to_run+2)
		# self.products["ETH"] = Product("ETH-USD",self.time_to_run)

	def start(self):
		loop = asyncio.get_event_loop()
		future = asyncio.Future()
		for key, product in self.products.items():
			product.run(future)
		loop.run_until_complete(future)
		# loop.close()

gdax = GDAX(12)
gdax.start()