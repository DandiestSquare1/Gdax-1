import websockets
import asyncio
from json import dumps,loads
from dateutil.parser import parse
from math import ceil
from time import time
from collections import OrderedDict,deque
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
					parse(message["time"]).timestamp(),
					message["side"]
					))

	async def on_interval(self,interval):
		# Expect the formula to produce the spread info to change in the future. Now it's simply giving center prices arbitrary more weight
		self.interval_book[interval] = OrderedDict()
		sign = lambda x: (x>0) - (x<0)
		# Long interval has larger fluctuation
		if interval <= 1:
			fluctuation = 0.004
		elif interval <= 30:
			fluctuation = 0.01
		elif interval <= 300:
			fluctuation = 0.05
		else:
			fluctuation = 0.1

		# text_file = open("Interval_Data_by_%s_Seconds.txt" % (interval), "w")
		# text_file.write("Time\topen_price\tLow\tWeighted_Price_Average\tHigh\tclose_price\tVolume\tBid_Price_Index\tAsk_Price_Index\tEMA_12\tEMA_26\tEMA_DIF\tMACD\tEMA_Up\tEMA_Down\tRSI\tTSI\tEMA_Volume_9\tOBV_Delta\n")

		# Sync with order book
		while self.Start_Time == 0:
			await asyncio.sleep(0.1)

		This_Moment = self.Start_Time
		await asyncio.sleep(This_Moment + interval - time())

		match_book_index = 0
		open_price = self.start_price
		while match_book_index < len(self.match_book) and self.match_book[match_book_index][2] < This_Moment:
			open_price = self.match_book[match_book_index][0]
			match_book_index += 1


		price_jump_5 = deque([],5)
		price_jump_30 = deque([],30)

		while True:
			Amount = 0
			Volume = 0
			Low = 999999
			High = 0
			while match_book_index < len(self.match_book) and self.match_book[match_book_index][2] < This_Moment + interval:
				close_price = self.match_book[match_book_index][0]
				Low = min(Low,close_price)
				High = max(High,close_price)
				Volume += self.match_book[match_book_index][1]
				Amount += self.match_book[match_book_index][0] * self.match_book[match_book_index][1]
				match_book_index += 1
			if Amount > 0:
				Volume = round(Volume,8)
				Weighted_Price_Average = round(Amount / Volume,2)
			else:
				Low = open_price
				High = open_price
				Weighted_Price_Average = open_price
				close_price = open_price

			if This_Moment == self.Start_Time:
				sigma_5 = 0
				sigma_30 = 0
				price_jump_5.append(High-Low)
				price_jump_30.append(High-Low)
				EMA_12 = Weighted_Price_Average
				EMA_26 = Weighted_Price_Average
				EMA_DIF = 0
				EMA_Up = max(0,close_price - open_price)
				EMA_Down = max(0,open_price - close_price)
				EMA_Momentum = close_price - open_price
				EMA_Momentum_Abs = abs(close_price - open_price)
				EMA_Momentum_2 = EMA_Momentum
				EMA_Momentum_Abs_2 = EMA_Momentum_Abs
				EMA_Volume_9 = 1
				EMA_Volume_99 = 1
				OBV_Delta = 0
			else:
				if High-Low > self.current_price / 10000:
					price_jump_5.append(High-Low)
					price_jump_30.append(High-Low)
					price_jump_avg_5 = sum(price_jump_5) / len(price_jump_5)
					price_jump_avg_30 = sum(price_jump_30) / len(price_jump_30)
					sigma_5 = (sum([(x - price_jump_avg_5) ** 2 for x in price_jump_5]) / len(price_jump_5)) ** 0.5
					sigma_30 = (sum([(x - price_jump_avg_30) ** 2 for x in price_jump_30]) / len(price_jump_30)) ** 0.5

				EMA_12 = round((1-2/(12+1)) * EMA_12 + 2/(12+1) * Weighted_Price_Average,6)
				EMA_26 = round((1-2/(26+1)) * EMA_26 + 2/(26+1) * Weighted_Price_Average,6)
				EMA_DIF = round((1-2/(9+1)) * EMA_DIF + 2/(9+1) * (EMA_12 - EMA_26),6)
				EMA_Up = round((1-1/14) * EMA_Up + 1/14 * max(0,close_price - open_price),6)
				EMA_Down = round((1-1/14) * EMA_Down + 1/14 * max(0,open_price - close_price),6)
				EMA_Momentum = (1-2/(25+1)) * EMA_Momentum + 2/(25+1) * (close_price - open_price)
				EMA_Momentum_Abs = (1-2/(25+1)) * EMA_Momentum_Abs + 2/(25+1) * abs(close_price - open_price)
				EMA_Momentum_2 = (1-2/(13+1)) * EMA_Momentum_2 + 2/(13+1) * EMA_Momentum
				EMA_Momentum_Abs_2 = (1-2/(13+1)) * EMA_Momentum_Abs_2 + 2/(13+1) * EMA_Momentum_Abs
				EMA_Volume_9 = (1-2/(9+1)) * EMA_Volume_9 + 2/(9+1) * Volume
				EMA_Volume_99 = (1-2/(99+1)) * EMA_Volume_99 + 2/(99+1) * Volume
				# OBV_Delta is generally used to confirm price moves. The idea is that volume is higher on days where the price move is in the dominant direction
				OBV_Delta = round(Volume * sign(close_price - open_price),8)
			volume_range = round(max(EMA_Volume_9,EMA_Volume_99) * 10,4)
			sigma = round(max(sigma_5,sigma_30)*10,2)

			MACD = round(EMA_12 - EMA_26 - EMA_DIF,6)
			RSI = 50 if EMA_Down == 0 else round((1-1/(1+(EMA_Up/EMA_Down))) * 100,2)
			TSI = 0 if EMA_Momentum_Abs_2 == 0 else round(EMA_Momentum_2 / EMA_Momentum_Abs_2 * 100,2)
			
			Bull_Momentum = 0
			Order_Volume = 0
			lower_bound = 0
			for price in self.order_book.irange(None, self.max_bid_price, (True,True),True):
				Order_Volume += self.order_book[price]
				if Order_Volume > volume_range and price < self.max_bid_price - sigma:
					lower_bound = price
					break
				Bull_Momentum += self.max_bid_price / (self.max_bid_price - price + max(sigma,2)) * self.order_book[price]
			
			Bear_Momentum = 0
			Order_Volume = 0
			upper_bound = 0
			for price in self.order_book.irange(self.min_ask_price,None, (True,True)):
				Order_Volume += self.order_book[price]
				if Order_Volume > volume_range and price > self.min_ask_price + sigma:
					upper_bound = price
					break
				Bear_Momentum += self.min_ask_price / (price - self.min_ask_price + max(sigma,2)) * self.order_book[price]

			if Bull_Momentum > Bear_Momentum:
				Predict_Price = upper_bound
			if Bull_Momentum < Bear_Momentum:
				Predict_Price = lower_bound

			Certainty = round(abs(Bull_Momentum - Bear_Momentum)/max(Bull_Momentum,Bear_Momentum),2)

			if This_Moment == self.Start_Time:
				EMA_Predict = Predict_Price
				EMA_Certainty = Certainty
			else:
				EMA_Predict = round((1 - Certainty) * EMA_Predict + Certainty * Predict_Price,2)
				EMA_Certainty = round((1-2/(9+1)) * EMA_Certainty + 2/(9+1) * Certainty,4)

			print(EMA_Predict,EMA_Certainty,volume_range,sigma)
			# self.interval_book[interval][This_Moment] = (This_Moment, open_price, Low, Weighted_Price_Average, High, close_price, Volume, Bid_Price_Index, Ask_Price_Index, EMA_12, EMA_26, EMA_DIF, MACD, EMA_Up,EMA_Down,RSI, TSI, EMA_Volume_9, OBV_Delta)
			# text_file.write(*self.interval_book[interval][This_Moment],sep = '\t')
			# print(self.interval_book[interval][This_Moment])
			# print(self.interval_book[interval][This_Moment][8],self.interval_book[interval][This_Moment][9])
			This_Moment += interval
			open_price = close_price
			await asyncio.sleep(This_Moment + interval - time())

			# text_file.write("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" % (This_Moment, open_price, Low, Weighted_Price_Average, High, close_price, Volume, Bid_Price_Index, Ask_Price_Index, EMA_12, EMA_26, EMA_DIF, MACD, EMA_Up,EMA_Down,RSI, TSI, EMA_Volume_9, OBV_Delta))

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
		# asyncio.ensure_future(self.on_interval(5))
		asyncio.ensure_future(self.stop_future(future))

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

gdax = GDAX(10000)
gdax.start()