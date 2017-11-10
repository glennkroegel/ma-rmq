#!/usr/bin/env python

'''
api_functions.py

@version: 1.0

Binary.com api functions. See https://developers.binary.com/

@author: Glenn Kroegel
@contact: glenn.kroegel@gmail.com

'''

import socket
import hashlib
import cgi
import threading
import time
import json
import threading
import pika
import ast
from StringIO import StringIO
import sys

#########################################################################################################################

api_token = '76O9rMbA7TmKcyD'
app_id = '2802'

#########################################################################################################################

# AUTHORIZED CALLS (No Impact)

def authorize(ws):

	data = {'authorize': api_token}
	message = json.dumps(data)
	ws.send(message)

def statement():

	data = {'statement': 1}
	message = json.dumps(data)

def balance(ws):

	data = {'balance': 1}
	message = json.dumps(data)
	ws.send(message)

def account_status():

	data = {'get_account_status': 1}
	message = json.dumps(data)

#########################################################################################################################

# UNAUTHENTICATED CALLS

def tick_history(ws, asset, end = "latest", count = 30, style = "candles", granularity = 60, req_id = 1):

	request = 	{
					"ticks_history": asset,
					"end": end,
					"count": count,
					"style": style,
					"granularity": granularity,
					"req_id": req_id
				}

	message = json.dumps(request)
	ws.send(message)

def server_time(ws):

	data = {'time': 1}
	message = json.dumps(data)
	ws.send(message)

#########################################################################################################################

# UNAUTHENTICATED STREAMS

def tick_stream(ws, asset = 'R_50'):

	message = json.dumps({'ticks': asset})
	ws.send(message)

def price_proposal(ws, data):

	message = json.dumps(data)
	ws.send(message)
