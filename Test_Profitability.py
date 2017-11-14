
import websockets
import asyncio
import pandas as pd
from json import dumps,loads
from dateutil.parser import parse
from math import ceil
from time import time
from collections import OrderedDict,deque
from sortedcontainers import SortedDict
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 10000)
pd.set_option('display.max_columns', None)
def test(interval,debug = True):
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

	# df["local_time"] = df["local_time"].map(lambda x: '{0:.17}'.format(x))
		
	order_file.close()
	match_file.close()

	# current_time is the time at the end of the interval
	current_time = ceil(df["local_time"][0])
	interval_volume = 0
	interval_amount = 0
	interval_open_price = max_bid_price
	interval_close_price = max_bid_price
	interval_avg_price = max_bid_price
	EMA_12 = max_bid_price
	EMA_26 = max_bid_price
	EMA_DIF = 0

	for index,local_time,book,price,volume,side in df.itertuples():
		if local_time >= current_time:
			# End of Interval Calculation
			if interval_amount > 0:
				interval_volume = round(interval_volume,8)
				interval_avg_price = round(interval_amount / interval_volume,2)

			EMA_12 = (1-2/(12+1)) * EMA_12 + 2/(12+1) * interval_avg_price
			EMA_26 = (1-2/(26+1)) * EMA_26 + 2/(26+1) * interval_avg_price
			EMA_DIF = (1-2/(9+1)) * EMA_DIF + 2/(9+1) * (EMA_12 - EMA_26)
			MACD = EMA_12 - EMA_26 - EMA_DIF

			print(EMA_DIF,MACD,interval_close_price, sep = "\t")

			# Initialization for the next interval
			interval_volume = 0
			interval_amount = 0
			interval_low = interval_close_price
			interval_high = interval_close_price
			interval_open_price = interval_close_price
			interval_avg_price = interval_open_price
			current_time += interval
			

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
			interval_volume += volume
			interval_amount += volume * price
			interval_low = min(interval_low,price)
			interval_high = max(interval_high,price)
			interval_close_price = price


test(10,False)
