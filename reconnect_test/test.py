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
	if(msg_type not in ['tick','balance','authorize']):
		print res

def on_close(ws):

	print("Websocket connection closed")
	logging.info("Connection closed")

def on_error(ws, error):

	print error

	if(ws is not None):
		ws.close()
		ws.on_close = None
		ws.on_message = None
		ws.on_open = None
		ws.close = None
		print("deleting websocket..")
		del ws
	
	ws = None
	apiURL = "wss://ws.binaryws.com/websockets/v3?app_id=2802" # hard coded app_id

	while True:
		try:
			ws = websocket.WebSocketApp(apiURL, on_message = on_message, on_close = on_close, on_error = on_error)
			print 'On_Error: After Creation-1'
			if(ws is not None):
				print 'After Creation -  inside on_error : on_open'
				ws.on_open = on_open
				ws.run_forever(ping_interval=30, ping_timeout=10, sslopt={"ssl_version": ssl.PROTOCOL_TLSv1_1})
				print ' WS is created after the error - successfully'
				break
		except Exception as e:
			print e

def main():

	websocket.enableTrace(False)
	websocket.setdefaulttimeout(10)
	apiURL = "wss://ws.binaryws.com/websockets/v3?app_id=2802" # hard coded app_id
	ws = websocket.WebSocketApp(apiURL, on_message = on_message, on_close = on_close, on_error = on_error)
	ws.on_open = on_open
	ws.run_forever(ping_interval=30, ping_timeout=10, sslopt={"ssl_version": ssl.PROTOCOL_TLSv1_1})


if __name__ == "__main__":

  try:

    main()

  except KeyboardInterrupt:

    print('Interupted...Exiting...')