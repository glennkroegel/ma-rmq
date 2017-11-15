'''
receiver.py

@version: 1.0

Receives message when at new bar/candle

@author: Glenn Kroegel
@contact: glenn.kroegel@gmail.com

CHANGE LOG - None 

INFO - https://github.com/Gsantomaggio/rabbitmqexample/blob/master/webSocketPython/my-server.py

to do - call last bar and append, calc probability for last X (compare results), retry limit + force request maximum to see if append helps

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
from threading import Thread

from calculations import *
from context import *

################################

asset = 'frxEURUSD'

# Context logger
print("Loading models...")
'''bar = ContextLogger()
balance = BalanceLogger()'''

################################

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

def threaded_rmq():
	channel.exchange_declare(exchange = 'bars', type = 'fanout')
	result = channel.queue_declare(exclusive=True)
	queue_name = result.method.queue
	channel.queue_bind(exchange='bars', queue=queue_name)
	channel.basic_consume(callback, queue=queue_name, no_ack=True)
	logging.info('consumer ready, on my_queue')
	print(' [*] Waiting for messages. To exit press CTRL+C')
	channel.start_consuming()

def disconnect_to_rmq():
	channel.stop_consuming()
	connection.close()
	logging.info('Disconnected from Rabbitmq')

def callback(ch, method, properties, body):
	print(" [x] Received %r" % body)
	# Update balance
	balance.set_balance(body)
	# get bars
	tick_history(ws, asset = asset, count = 200)


################################

# Websocket events

def on_open(ws):

	print("Server connected")
	logging.info("Server connected")
	global bar
	global balance
	bar = ContextLogger()
	balance = BalanceLogger()
	authorize(ws)
	#tick_history(ws, asset = asset, count = 200)


def on_message(ws, message):

	res = json.loads(message.decode('utf8'))
	msg_type = res['msg_type']
	#print res

	if(msg_type == 'authorize'):
		print res
		# on start - populate historical data
		tick_history(ws, asset = asset, count = 200)
		# start tick stream?
		tick_stream(ws)

	if(msg_type == 'tick'):
		epoch_tick = res['tick']['epoch']
		dt_tick = dt.datetime.utcfromtimestamp(int(epoch_tick))
		if(dt_tick.second == 0):
			print(" [x] {0} : {1}".format(epoch_tick,balance))
			tick_history(ws, asset = asset, count = 200) # request new candle and make prediction on response
		if(dt_tick.second == 30):
			message = json.dumps({'balance': 1}) # get latest balance
			ws.send(message)

	if(msg_type == 'balance'):
		res_balance = res['balance']['balance']
		balance.set_balance(res_balance)
		print("balance: {0}".format(str(balance.get_balance())))

	if(msg_type == 'candles'):
		if(bar.X is not None):
			if(bar.on_bar(res['candles'])==True):
				print('{0},{1}'.format(bar.time, bar.px))
				logging.info('{0},{1}'.format(bar.time, bar.px))
				trade = TradeHandler(asset, bar.X, bar.px, balance.get_balance(), threshold_up = 0.59, threshold_down = 0.41, proportion = 0.01)
				trade.on_bar()
				if(trade.execute == True):
					msg = json.dumps(trade.proposal)
					ws.send(msg)
			else:
				print('retrying...')
				logging.info('retrying...')
				print bar.retry_attempts
				if bar.retry_attempts < 3:
					# Retry
					time.sleep(1)
					tick_history(ws, asset=asset, count=180)
				else:
					# Reconnect

					if(ws is not None):
						ws.close()
						ws.on_message = None
						ws.on_close = None
						ws.close = None
						print("deleting websocket..")
						del ws
						logging.info('Websocket deleted')

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
								bar.restart()
								print ' WS is created after the error - successfully'
								logging.info('Websocket restarted')
								break
						except Exception as e:
							print e
							logging.info(str(e))

		if(bar.X is None):
			# starting
			print('populating data..')
			bar.on_start(res['candles'])
	
	if(msg_type == 'proposal'):
		proposal = ProposalHandler(res, min_payout=0.75, max_delay=10)
		if(proposal.execute == True):
			msg = json.dumps(proposal.execute_request())
			ws.send(msg)
		else:
			logging.info('proposal rejected: payout {0}, delay {1}'.format(proposal.payout, proposal.delay))

	if(msg_type == 'buy'):
		print res
		purchase_time = dt.datetime.utcfromtimestamp(int(res['buy']['purchase_time'])).strftime('%Y-%m-%d %H:%M:%S')   
		purchase_shortcode = res['buy']['shortcode']
		logging.info('trade executed at {0} - {1}'.format(purchase_time, purchase_shortcode))
		print('trade executed at {0} - {1}'.format(purchase_time, purchase_shortcode))
		

	if(msg_type == 'error'):
		print res

def on_close(ws):

	print("Websocket connection closed")
	logging.info("Connection closed")

def on_error(ws, error):

	print error

	if(ws is not None):
		ws.close()
		ws.on_message = None
		ws.on_close = None
		ws.close = None
		print("deleting websocket..")
		del ws
		logging.info('Websocket deleted')

	# Reconnect
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
				logging.info('Websocket restarted')
				break
		except Exception as e:
			print e
			logging.info(str(e))

################################

websocket.enableTrace(False)
websocket.setdefaulttimeout(10)
apiURL = "wss://ws.binaryws.com/websockets/v3?app_id=2802" # hard coded app_id
ws = websocket.WebSocketApp(apiURL, on_open = on_open, on_message = on_message, on_close = on_close, on_error = on_error)

def start_websocket():
	ws.run_forever(ping_interval=30, ping_timeout=10, sslopt={"ssl_version": ssl.PROTOCOL_TLSv1_1}) # check options here - ws.run_forever(ping_interval=30, ping_timeout=10)

def stop_websocket():
	ws.keep_running = False


################################

def main():

	# Logging
	logging.basicConfig(filename = 'receiver.log', format = "%(asctime)s; %(message)s", datefmt = "%Y-%m-%d %H:%M:%S", level = logging.INFO)
	global balance

	# Message queue
	print('Starting thread RabbitMQ')
	threadRMQ = Thread(target=threaded_rmq)
	threadRMQ.start()

	# Websocket
	print('Starting websocket..')
	threadWS = Thread(target=start_websocket)
	threadWS.start()

if __name__ == "__main__":

  try:

    main()

  except KeyboardInterrupt:

    print('Interupted...Exiting...')
