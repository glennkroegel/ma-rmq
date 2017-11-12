#!/opt/conda/bin/python

'''
calls.py

@version: 1.0

Real time model calculation and execution using binary.com API

@author: Glenn Kroegel
@contact: glenn.kroegel@gmail.com

CHANGE LOG - Fixed expiry time added 

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

#from Functions import *
from calculations import *

#########################################################################################################################

model = joblib.load('model.pkl')
clients = []

#########################################################################################################################

# MODEL FEATURES

def calcFeaturesLocally(df, asset = 'frxEURGBP'):

	df = copy.deepcopy(df)
	df = np.round(df, decimals = 8)

	if asset == 'frxEURGBP':
		df['WILLR'] = taCalcIndicator(df, 'WILLR', window = 30)
		df['WILLR_D1'] = df['WILLR'].diff()
		df['WILLR_D2'] = df['WILLR'].diff(2)
		df['WILLR_D5'] = df['WILLR'].diff(5)
		df['RSI'] = taCalcIndicator(df, 'RSI', window = 30)
		return df
	else:
		logging.info('No features defined for asset')
		pass

#########################################################################################################################

# TRADE ACTIONS

def tradeActions(asset, px, amount, dt_last_bar, passthrough):

	px = float(px)
	dt_last_bar = str(dt_last_bar)

	offset = 60
	delta = 300 + offset
	#print dt_last_bar
	dt_last_bar = dt.datetime.strptime(dt_last_bar, '%Y-%m-%d %H:%M:%S')
	t = dt_last_bar + dt.timedelta(seconds = delta) - dt.datetime(1970,1,1)
	#print t
	t = int(t.total_seconds())
	# print dt_last_bar
	# print t
	# print(pd.to_datetime(t, unit = 's'))

	proposal = 	{
			        "proposal": 1,
			        "amount": amount,
			        "basis": "stake",
			        "contract_type": "CALL",
			        "currency": "USD",
			        "date_expiry": t,
			        "symbol": asset,
			        "passthrough": {"last_close": passthrough}
				}

	if asset == 'frxEURGBP':

		if (px > 0.67):
			proposal['contract_type'] = "CALL"
			return proposal
		elif (px < 0.33):
			proposal['contract_type'] = "PUT"
			return proposal
		else:
			return None
	else:
		logging.info('No trade actions defined for asset')
		return None


def marketConditions(data, asset):

	if asset == 'frxEURGBP':

		c1 = True #data['NEWMINMAX'].ix[-1:].values == 4
		c2 = (time.gmtime().tm_hour >= 21) & (time.gmtime().tm_hour < 23) # CHECK CHECK CHECK

		if (c1 == True) & (c2 == True):
			return True
		else:
			return False
	else:
		logging.info('No market condition for asset')
		return False

#########################################################################################################################

# VALIDITY CHECKS

def dataCheck(ls_x):

	c1 = np.isnan(ls_x).any()
	c2 = np.isinf(ls_x).any()

	if (c1 == True) or (c2 == True):
		logging.info('Data check failed')
		return False
	else:
		return True

def timeCheck(dt_time, delta = 1):

	# FIX

	dt_now = pd.to_datetime(time.time(), unit = 's')
	dt_last_bar = dt_time

	c1 = True
	c2 = True

	if (c1 == True) & (c2 == True):
		return True
	else:
		logging.info('Time check failed')
		return False

def timeCheck2(dt_now, dt_proposal, max_delay = 20):

	#dt_now = pd.to_datetime(time.time(), unit = 's') # investigate date_start on proposal response
	dt_now = pd.to_datetime(int(dt_now), unit = 's')
	dt_proposal = pd.to_datetime(int(dt_proposal), unit = 's')
	delay = dt_now - dt_proposal

	c1 = delay.seconds < max_delay 
	c2 = True

	if (c1 == True) & (c2 == True):
		return True
	else:
		logging.info('Time check (2) failed')
		return False

def payoutCheck(ask_price, payout_price, min_payout = 0.65):

	# ask_price - contract stake / trade amount
	# payout_price - return value of contract : ask_price + profit

	ask_price = float(ask_price)
	payout_price = float(payout_price)

	payout = (payout_price - ask_price)/ask_price # placeholder - FIX

	if (payout >= min_payout):
		return True
	else:
		logging.info('Payout too low: {0}'.format(payout))
		return False

def spotCheck(close_price, spot_price, asset):

	# close_price - close price from last bar used in model calculation
	# spot_price - current spot price for contract

	
	if asset == 'frxEURGBP':
		error_threshold = 0.00005
	else:
		logging.info('No spot check for asset')
		error_threshold = 0.0001

	delta = np.absolute(float(close_price) - float(spot_price))

	if (delta < error_threshold):
		return True
	else:
		logging.info('Price moved')
		return False

#########################################################################################################################

# FUNCTIONS

def formatBars(data):

	# Receive JSON bars - Format bars into dataframe with correct time index

	df_bars = pd.DataFrame(data)

	df_bars = df_bars.rename(columns = {'epoch': 'DATETIME', 'open': 'OPEN', 'high': 'HIGH', 'low': 'LOW', 'close': 'CLOSE'})
	df_bars['DATETIME'] = pd.to_datetime(df_bars['DATETIME'], unit = 's')
	df_bars = df_bars.set_index('DATETIME')
	df_bars['VOLUME'] = np.zeros(df_bars['CLOSE'].shape)

	df_bars = pd.DataFrame(df_bars, dtype = 'float')
	df_bars = df_bars.head(len(df_bars)-1)

	return df_bars

def onBar(data, time_delta = 60):

	try:
		dt_now = dt.datetime.utcfromtimestamp(int(data['req_id']))
		dt_current_bar = dt.datetime.utcfromtimestamp(int(data['candles'][-1]['epoch']))
		dt_last_bar = dt.datetime.utcfromtimestamp(int(data['candles'][-2]['epoch']))

		c1 = dt_now == dt_current_bar # ensuring that bar is complete by checking that there is a new incomplete bar
		c2 = (dt_now - dt_last_bar).seconds == time_delta

		if (c1 == True) & (c2 == True):
			return True
		else:
			return False
	except:
		logging.info('OnBar error')
		return False

def getAmount(balance, proportion):

	amount = np.round(proportion*balance)

	if (amount > 10000):
		return 10000
	elif (amount < 10):
		return 10
	else:
		return amount

def send(ws):

	# remember to test multiple streams of shit

	authorize(ws)
	tick_stream(ws)

	#tick_history(ws, asset = "R_50")
	#server_time(ws)

#########################################################################################################################

# WEBSOCKET 

def on_open(ws):

	print("Server connected")
	logging.info("Server connected")
	send(ws)

def on_message(ws, message):

	res = json.loads(message.decode('utf8'))
	msg_type = res['msg_type']
	asset = 'frxEURGBP'

	#print("Message received: {0}".format(res))

	# CASES

	if (msg_type == 'authorize'):
		global start_balance
		global current_balance
		global trade_proportion
		global trade_x

		try:
			start_balance = float(res['authorize']['balance'])
			current_balance = start_balance
			trade_proportion = 0.05
			trade_x = getAmount(current_balance, trade_proportion)

			print("Session authorized")
			logging.info("Authorization successful")
		except:
			logging.info("Authorization failed")
			ws.close()

	elif (msg_type =='candles'):

		if onBar(res) == True:
			
			order = None
			df_bars = formatBars(res['candles']) # check this

			# FEATURE CALCULATION

			df_features = calcFeaturesLocally(df_bars, asset = asset)
			df_features.to_csv('vector2.csv')

			ls_exlude = ['OPEN','HIGH','LOW','CLOSE','VOLUME']
			cols = [col for col in df_features.columns if col not in ls_exlude]
			df_x = df_features[cols]

			# PROBABILITY CALCULATION

			if marketConditions(df_x, asset = asset) == True:

				ls_x = df_x.iloc[-1:].as_matrix()

				if (dataCheck(ls_x) == False) or (timeCheck(ls_x) == False):
					lr_px = np.array([(0.5,0.5)])
					str_px = str(lr_px[:,1][0])
					str_date = str(df_x.ix[-1:].index.format()[0])
					print("{0}: {1}".format(str_date, str_px))
				else:
					try:
						lr_px = model.predict_proba(ls_x)
						str_px = str(lr_px[:,1][0])
						str_date = str(df_x.ix[-1:].index.format()[0])
						str_close = str(df_features['CLOSE'].ix[-1:][0])
						print("{0}: {1}, Close: {2}".format(str_date, str_px, str_close))
						logging.info("{0}: {1}, Close: {2}".format(str_date, str_px, str_close))
						order = tradeActions(asset, str_px, amount = trade_x, dt_last_bar = str_date, passthrough = str_close)
					except Exception,e:
						lr_px = np.array([(0.5,0.5)])
						order = None
						print e
						logging.info('Model prediction failed: {0}'.format(e))
			else:
				lr_px = np.array([(0.5,0.5)])
				str_px = str(lr_px[:,1][0])
				str_date = str(df_x.ix[-1:].index.format()[0])
				print("{0}: {1}".format(str_date, str_px))
				logging.info("{0}: {1}".format(str_date, str_px))

			# SEND ORDER

			if order is not None:
				try:
					message = json.dumps(order)
					ws.send(message)
				except:
					print("Order failed to send")
					logging.info('Order failure - {0} - {1}'.format(str_date, str_px))
			else:
				pass
		else:
			#logging.info('Candle unavailable. Retrying...')
			try:
				time.sleep(0.4)
				tick_history(ws, asset = asset, count = 120, req_id = res['req_id'])
			except:
				logging.info('Retry failed')
	elif (msg_type == 'proposal'):
		try:
			proposal_id = res['proposal']['id']
			proposal_asset = res['echo_req']['symbol']		
			proposal_payout = res['proposal']['payout']
			proposal_amount = res['proposal']['ask_price']
			proposal_spot = res['proposal']['spot']
			proposal_start_time = res['proposal']['date_start']			
			proposal_spot_time = res['proposal']['spot_time']			
			passthrough_close = res['echo_req']['passthrough']['last_close'] # from passthrough
			trade_amount = trade_x

			if (payoutCheck(proposal_amount, proposal_payout) == True) and (spotCheck(passthrough_close, proposal_spot, asset = proposal_asset) == True) and (timeCheck2(proposal_start_time, proposal_spot_time) == True):
				message = json.dumps({'buy': proposal_id, 'price': trade_amount, 'passthrough': {'entry_price': proposal_spot}})
				ws.send(message)
			else:
				logging.info('Trade skipped')
		except:
			proposal_error = res['error']['code']
			logging.info('Proposal response error: {0}'.format(proposal_error))
	elif (msg_type == 'buy'):
		try:
			purchase_time = dt.datetime.utcfromtimestamp(int(res['buy']['purchase_time'])).strftime('%Y-%m-%d %H:%M:%S')   
			purchase_shortcode = res['buy']['shortcode']
			entry_price = res['echo_req']['passthrough']['entry_price']
			logging.info('{0}: {1}: {2}'.format(purchase_time, purchase_shortcode, entry_price))
			print('{0}: {1}: {2}'.format(purchase_time, purchase_shortcode, entry_price))
		except:
			buy_error = res['error']['code']
			logging.info('Buy response error: {0}'.format(buy_error))
	elif (msg_type == 'tick'):
		epoch_tick = res['tick']['epoch']
		dt_tick = dt.datetime.utcfromtimestamp(int(epoch_tick))
		if (dt_tick.second == 0):
			tick_history(ws, asset = asset, count = 200, req_id = epoch_tick)
		elif (dt_tick.second == 30):
			try:
				message = json.dumps({'balance': 1})
				ws.send(message)
			except:
				trade_x = 1
				logging.info('Failure on balance request')
		else:
			pass
	elif (msg_type == 'balance'):
		try:
			current_balance = float(res['balance']['balance'])
			trade_x = getAmount(current_balance, trade_proportion)
			print("Balance: {0} Trade: {1}".format(current_balance, trade_x))
			#logging.info("Balance: {0} Trade: {1}".format(current_balance, trade_x))
		except:
			trade_x = 1
			logging.info('Failure on balance response')
	elif (msg_type == 'time'):
		dt_server_time = dt.datetime.utcfromtimestamp(int(res['time'])).strftime('%Y-%m-%d %H:%M:%S')
		logging.info("{0}".format(dt_server_time))
	else:
		print("Message received: {0}".format(res))
		#ws.close()
		pass

def on_error(ws, error):

	print("Websocket error: {0}".format(error))

def on_close(ws):

	print("Websocket connection closed")
	logging.info("Connection closed")

def main():

	#######################################################

	# LOG FILE

	logging.basicConfig(filename = 'CALLS_EURGBP2.log', format = "%(asctime)s; %(message)s", datefmt = "%Y-%m-%d %H:%M:%S", level = logging.DEBUG)

	#######################################################

	print('Starting websocket..')

	websocket.enableTrace(False)
	apiURL = "wss://ws.binaryws.com/websockets/v3?app_id=2802" # hard coded app_id
	ws = websocket.WebSocketApp(apiURL, on_message = on_message, on_error = on_error, on_close = on_close)
	ws.on_open = on_open
	ws.run_forever(sslopt={"ssl_version": ssl.PROTOCOL_TLSv1_1})



if __name__ == "__main__":

  try:

    main()

  except KeyboardInterrupt:

    print('Interupted...Exiting...')

