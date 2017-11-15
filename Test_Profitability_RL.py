
import websockets
import asyncio
import pandas as pd
from json import dumps,loads
from dateutil.parser import parse
from math import ceil
from time import time
from collections import OrderedDict,deque
from sortedcontainers import SortedDict
from pprint import pprint
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', None)
def test(intervals,debug = True):
	if debug:
		n = 1000
	else:
		n = None

	order_book = SortedDict()
	match_book = []

	order_file = open("data\order_book.txt")
	match_file = open("data\match_book.txt")
	json_message = loads(order_file.readline().replace("'",'"'))
	max_bid_price = 0
	min_ask_price = 0
	for [x,y] in json_message["asks"]:
		order_book[float(x)] = float(y)
		min_ask_price = float(x) if min_ask_price == 0 else min_ask_price
	for [x,y] in json_message["bids"]:
		order_book[float(x)] = float(y)
		max_bid_price = float(x) if max_bid_price == 0 else max_bid_price
	
	df = pd.concat([
		pd.read_csv(order_file, sep = '\t', nrows = n, skiprows = 0, names = ["side","price","volume","local_time"]).assign(book = "order")
		,pd.read_csv(match_file, sep = '\t', nrows = n, names = ["type","side","price","volume","server_time","local_time"], usecols = [1,2,3,5]).assign(book = "match")
		]).reindex(["local_time","book","price","volume","side"],axis = 1)
	df.sort_values("local_time",inplace = True, kind = 'mergesort')
	df.reset_index(drop = True, inplace = True)
		
	order_file.close()
	match_file.close()

	# current_time is the time at the end of the interval
	interval_data = {}
	for i in intervals:
		interval_data[i] = {}
		interval_data[i]["current_time"] = ceil(df["local_time"][0])
		interval_data[i]["volume"] = 0
		interval_data[i]["amount"] = 0
		interval_data[i]["open_price"] = max_bid_price
		interval_data[i]["close_price"] = max_bid_price
		interval_data[i]["avg_price"] = max_bid_price
		interval_data[i]["Volume_Range"] = round(i**0.6 * 5,2)
		interval_data[i]["EMA_Volume"] = 0
		interval_data[i]["EMA_12"] = max_bid_price
		interval_data[i]["EMA_26"] = max_bid_price
		interval_data[i]["EMA_DIF"] = 0


	for index,local_time,book,price,volume,side in df.itertuples():
		for interval,data in interval_data.items():
			while local_time >= data["current_time"]:
				# End of Interval Calculation
				if data["amount"] > 0:
					data["volume"] = round(data["volume"],8)
					data["avg_price"] = round(data["amount"] / data["volume"],2)

					if data["volume"] > 0.001:
						data["EMA_Volume"] = 4/5 * data["EMA_Volume"] + data["volume"] * 3 / 5
					else:
						data["EMA_Volume"] = 19/20 * data["EMA_Volume"] + data["Volume_Range"] / 20

				data["EMA_12"] = (1-2/(12+1)) * data["EMA_12"] + 2/(12+1) * data["avg_price"]
				data["EMA_26"] = (1-2/(26+1)) * data["EMA_26"] + 2/(26+1) * data["avg_price"]
				data["EMA_DIF"] = (1-2/(9+1)) * data["EMA_DIF"] + 2/(9+1) * (data["EMA_12"] - data["EMA_26"])
				data["MACD"] = data["EMA_12"] - data["EMA_26"] - data["EMA_DIF"]

				# print(EMA_DIF,MACD,interval_close_price, sep = "\t")
				print(interval, data["current_time"], data["volume"], data["EMA_Volume"], sep = "\t")

				# Initialization for the next interval
				data["current_time"] += interval
				data["volume"] = 0
				data["amount"] = 0
				data["low"] = data["close_price"]
				data["high"] = data["close_price"]
				data["open_price"] = data["close_price"]
				data["avg_price"] = data["open_price"]

		# New Order
		if book == "order":
			if volume == 0:
				del order_book[price]
			else:
				order_book[price] = volume
				if side == "buy":
					max_bid_price = max(max_bid_price,price)
				else:
					min_ask_price = min(min_ask_price,price)
				spread = min_ask_price - max_bid_price
		# New Match
		else:
			match_book.append((price, volume, local_time, side))
			for interval,data in interval_data.items():
				data["volume"] += volume
				data["amount"] += volume * price
				data["low"] = min(data["low"],price)
				data["high"] = max(data["high"],price)
				data["close_price"] = price


test([1,5,10,30,60],False)
