'''
signal_bar.py

@version: 1.0

Sends message when at new bar/candle

@author: Glenn Kroegel
@contact: glenn.kroegel@gmail.com

CHANGE LOG - None 

'''

import socket
import hashlib
import cgi
import time
import json
import pika
import datetime as dt
import ast
import websocket
import pandas as pd
import numpy as np
import ssl
from StringIO import StringIO
from api_functions import *
import sys
import logging
from sklearn.externals import joblib

# CUSTOM IMPORTS
from calculations import *

# Message queue
print('Opening message queue...')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange = 'bars', type = 'fanout')

def on_open(ws):

	print("Server connected")
	logging.info("Server connected")

	# Authorize stream
	authorize(ws)

	# Start stream
	#time.sleep(1)
	tick_stream(ws)

def on_message(ws, message):

	res = json.loads(message.decode('utf8'))
	msg_type = res['msg_type']

	if(msg_type == 'tick'):
		epoch_tick = res['tick']['epoch']
		dt_tick = dt.datetime.utcfromtimestamp(int(epoch_tick))
		if (dt_tick.second == 0):
			message = str(balance)
			channel.basic_publish(exchange='bars',routing_key='',body=message)
			print(" [x] {0} : {1}".format(epoch_tick,balance))
		if (dt_tick.second == 30):
			message = json.dumps({'balance': 1})
			ws.send(message)
	if(msg_type == 'balance'):
		balance = res['balance']['balance']
	if(msg_type == 'authorize'):
		# Get start balance
		global balance
		message = json.dumps({'balance': 1})
		ws.send(message)

def on_close(ws):

	print("Websocket connection closed")
	logging.info("Connection closed")

def main():

	#######################################################

	# LOG FILE

	logging.basicConfig(filename = 'BAR.log', format = "%(asctime)s; %(message)s", datefmt = "%Y-%m-%d %H:%M:%S", level = logging.DEBUG)

	#######################################################

	print('Starting websocket..')

	websocket.enableTrace(False)
	apiURL = "wss://ws.binaryws.com/websockets/v3?app_id=2802" # hard coded app_id
	ws = websocket.WebSocketApp(apiURL, on_message = on_message, on_close = on_close)
	ws.on_open = on_open
	ws.run_forever(sslopt={"ssl_version": ssl.PROTOCOL_TLSv1_1})


if __name__ == "__main__":

  try:

    main()

  except KeyboardInterrupt:

    print('Interupted...Exiting...')