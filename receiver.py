'''
receiver.py

@version: 1.0

Receives message when at new bar/candle

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

from calculations import *
from context import *

################################

asset = 'frxEURUSD'

################################

# Functions

def on_open(ws):

	print("Server connected")
	logging.info("Server connected")
	authorize(ws)

def on_message(ws, message):

	res = json.loads(message.decode('utf8'))
	msg_type = res['msg_type']

	if(msg_type == 'authorize'):
		#print(' [*] Waiting for messages. To exit press CTRL+C')
		#channel.start_consuming()
		print res

	if(msg_type == 'candles'):
		print res
	else:
		print res

def on_close(ws):

	print("Websocket connection closed")
	logging.info("Connection closed")

################################

def callback(ch, method, properties, body):
	print(" [x] Received %r" % body)
	# get bars
	tick_history(ws, asset = asset, count = 80)
	# do calc here


################################

# Logging
logging.basicConfig(filename = 'receiver.log', format = "%(asctime)s; %(message)s", datefmt = "%Y-%m-%d %H:%M:%S", level = logging.DEBUG)

# Message queue
print('Opening message queue...')
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.exchange_declare(exchange = 'bars', type = 'fanout')
result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='bars', queue=queue_name)
channel.basic_consume(callback, queue=queue_name, no_ack=True)
print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()

# Websocket
print('Starting websocket..')
websocket.enableTrace(False)
apiURL = "wss://ws.binaryws.com/websockets/v3?app_id=2802" # hard coded app_id
ws = websocket.WebSocketApp(apiURL, on_message = on_message, on_close = on_close)
ws.on_open = on_open
ws.run_forever(sslopt={"ssl_version": ssl.PROTOCOL_TLSv1_1})








'''def main():

	#######################################################

	# LOG FILE

	logging.basicConfig(filename = 'asset.log', format = "%(asctime)s; %(message)s", datefmt = "%Y-%m-%d %H:%M:%S", level = logging.DEBUG)

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

    print('Interupted...Exiting...')'''