from random import random
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
import matplotlib.pyplot as plt
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
	df = df[df["local_time"] < df["local_time"][0] + 12000]
		
	order_file.close()
	match_file.close()

	sign = lambda x: (x>0) - (x<0)
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
		interval_data[i]["MACD"] = 0
		interval_data[i]["MACD_Extrema"] = (0,0)
		interval_data[i]["DIF_Extrema"] = (0,0)
		interval_data[i]["USD"] = 10000
		interval_data[i]["BTC"] = 0
		interval_data[i]["_x"] = []
		interval_data[i]["_EMA_DIF"] = []
		interval_data[i]["_MACD"] = []
		interval_data[i]["_price"] = []

	for index,local_time,book,price,volume,side in df.itertuples():
		for interval,data in interval_data.items():
			while local_time >= data["current_time"]:
				# End of Interval Calculation
				if data["amount"] > 0:
					data["volume"] = round(data["volume"],8)
					data["avg_price"] = round(data["amount"] / data["volume"],2)

					if data["volume"] >= 0.001:
						data["EMA_Volume"] = 4/5 * data["EMA_Volume"] + data["volume"] * 3 / 5
					else:
						data["EMA_Volume"] = 19/20 * data["EMA_Volume"] + data["Volume_Range"] / 20
					# print(interval,data["current_time"],data["high"]-data["low"], sep = "\t")

				data["EMA_12"] = (1-2/(12+1)) * data["EMA_12"] + 2/(12+1) * data["avg_price"]
				data["EMA_26"] = (1-2/(26+1)) * data["EMA_26"] + 2/(26+1) * data["avg_price"]
				data["EMA_DIF"] = (1-2/(9+1)) * data["EMA_DIF"] + 2/(9+1) * (data["EMA_12"] - data["EMA_26"])
				data["MACD"] = data["EMA_12"] - data["EMA_26"] - data["EMA_DIF"]


				data["_x"].append(data["current_time"])
				data["_price"].append(data["close_price"])
				data["_MACD"].append(data["MACD"])
				data["_EMA_DIF"].append(data["EMA_DIF"])


				# if (sign(data["EMA_DIF"]) != sign(data["DIF_Extrema"][1])) or (abs(data["EMA_DIF"]) > abs(data["DIF_Extrema"][1])):
				# 	data["DIF_Extrema"] = (data["current_time"],data["EMA_DIF"])
				# if (sign(data["MACD"]) != sign(data["MACD_Extrema"][1])) or (abs(data["MACD"]) > abs(data["MACD_Extrema"][1])):
				# 	data["MACD_Extrema"] = (data["current_time"],data["MACD"])

				# if data["current_time"] > ceil(df["local_time"][0]) + interval * 30:
				# 	if abs(data["MACD"]) < 0.5 and abs(data["MACD_Extrema"][1]) > 2.5 and (data["current_time"] - data["MACD_Extrema"][0]) > 0:
				# 	# Buy
				# 	# if Bull_Momentum > Bear_Momentum and Certainty > 0.5:
				# 		if ((data["MACD"] - data["MACD_Extrema"][1]) / ((data["current_time"] - data["MACD_Extrema"][0])) > 0.02):
				# 			if data["USD"] > 0:
				# 				quantity = data["USD"]**2 / (data["USD"] + data["BTC"] * data["close_price"]) / 5
				# 				data["USD"] -= quantity
				# 				data["BTC"] += quantity / data["close_price"]
				# 	# Sell
				# 	# if Bull_Momentum < Bear_Momentum and Certainty > 0.5:
				# 		if ((data["MACD"] - data["MACD_Extrema"][1]) / ((data["current_time"] - data["MACD_Extrema"][0])) < -0.02):
				# 			if data["BTC"] > 0:
				# 				quantity = (1 - data["USD"] / (data["USD"] + data["BTC"] * data["close_price"])) / 5 * data["BTC"]
				# 				data["USD"] += quantity * data["close_price"]
				# 				data["BTC"] -= quantity

				# data["Worth"] = data["USD"] + data["BTC"] * data["close_price"]

				# print(data["current_time"], data["close_price"], data["MACD"],data["MACD_Extrema"][1],data["EMA_DIF"],data["DIF_Extrema"][1], sep= "\t")
				# (data["USD"],data["BTC"],data["Worth"])
				# print(EMA_DIF,MACD,interval_close_price, sep = "\t")
				# print(interval, data["current_time"], data["volume"], data["EMA_Volume"], data["EMA_DIF"], sep = "\t")

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


	fig, ax1 = plt.subplots()
	ax2 = plt.twinx(ax1)
	ax1.plot(interval_data[5]["_x"],interval_data[5]["_price"],'k', linewidth = .66)

	ax2.axhline(0, color='k',alpha = 0.3,linewidth = 0.5)

	MACD_5s = ax2.fill_between(interval_data[5]["_x"], interval_data[5]["_MACD"], 0, facecolor='#f18c26', alpha = 0.8,label = "MACD_5s")
	DIF_5s = ax2.fill_between(interval_data[5]["_x"], interval_data[5]["_EMA_DIF"], 0, facecolor='#f2ee26', alpha = 0.6,label = "DIF_5s")


	MACD_120s =  ax2.fill_between(interval_data[120]["_x"], interval_data[120]["_MACD"], 0, facecolor='#b9bece', alpha = 0.5,label = "MACD_120s")
	DIF_120s = ax2.fill_between(interval_data[120]["_x"], interval_data[120]["_EMA_DIF"], 0, facecolor="#e2e2e2", alpha = 0.3,label = "DIF_120s")
	
	plt.legend(handles = [MACD_5s,DIF_5s,MACD_120s,DIF_120s],loc = 3)
	plt.show()
# test([1,5,10,30,60],False)
test([5,120],False)
