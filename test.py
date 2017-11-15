import websockets
import asyncio
from json import dumps,loads
from dateutil.parser import parse
from math import ceil
from time import time
from collections import OrderedDict,deque
from sortedcontainers import SortedDict


def test():
	d = deque([1,2])
	d2 = d.copy()
	d2.append(3)

	print(d2)

test()
