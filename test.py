import websockets
import asyncio
from json import dumps,loads
from dateutil.parser import parse
from math import ceil
from time import time
from collections import OrderedDict,deque
from sortedcontainers import SortedDict


def test():
	match_book_file = open("data\match_book2.txt","w")
	match_book_file.close()

test()
