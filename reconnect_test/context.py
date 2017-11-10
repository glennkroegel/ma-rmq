'''
context.py

@version: 1.0

Manages and processes API data

@author: Glenn Kroegel
@contact: glenn.kroegel@gmail.com

CHANGE LOG - Fixed expiry time added 

to do - do bar check (sleep not enough)

'''

import numpy as np
import pandas as pd
from sklearn.externals import joblib
from calculations import *



class ContextLogger(object):
	"""docstring for ContextLogger"""
	def __init__(self):
		self.input = None
		self.dataframe = None
		self.X = None
		self.X_current = None
		self.time = None
		
		self.price_info_cols = ['OPEN','HIGH','LOW','CLOSE','VOLUME']
		self.feature_cols = None

		self.model = joblib.load('model.pkl')
		self.scaler = None #joblib.load('scaler.pkl')
		self.px = None

		self.retry_attempts = 0

	def on_start(self, message):

		# Placeholder method in case historical data for X can only be built once on start with a one row append on_bar

		# Populate historical data

		self.input = message
		
		dataframe = self.format_input(message)
		self.dataframe = self.calculate_features(dataframe)

		# Health check here?

		self.feature_cols = [col for col in self.dataframe.columns if col not in self.price_info_cols]
		self.X = self.dataframe[self.feature_cols]
		self.time = pd.to_datetime(self.X.index[-1], format='%Y-%m-%d %H:%M:%S')

		print self.X.tail()
		print self.time

	def on_bar(self, message):

		if self.bar_available(message):

			self.input = message
		
			dataframe = self.format_input(message)
			self.dataframe = self.calculate_features(dataframe)

			# Health check here?

			self.feature_cols = [col for col in self.dataframe.columns if col not in self.price_info_cols]
			self.X = self.dataframe[self.feature_cols]
			self.time = pd.to_datetime(self.X.index[-1], format='%Y-%m-%d %H:%M:%S')
			#print self.X.tail(3)
			#self.dataframe.to_csv('test.csv')

			if self.tradeable(self.X) == True:
				self.px = self.predict(self.X)
			else:
				self.px = 0.5

			self.set_retry_attempts(0)

			return True
		else:
			self.increment_retry_attempts()
			return False

	def bar_available(self, message, delta_expectation = 60):

		# time values of last two bars
		last_bar = self.time
		current_bar = pd.to_datetime(message[-2]['epoch'], unit='s') # get last epoch from message
		#print last_bar
		#print current_bar

		delta = (current_bar-last_bar).seconds

		if(delta==delta_expectation):
			return True
		else:
			return False

	def format_input(self, message):

		df = pd.DataFrame(message)
		df = df.rename(columns = {'epoch': 'DATETIME', 'open': 'OPEN', 'high': 'HIGH', 'low': 'LOW', 'close': 'CLOSE'})
		df['DATETIME'] = pd.to_datetime(df['DATETIME'], unit = 's')
		df = df.set_index('DATETIME')
		df['VOLUME'] = np.zeros(df['CLOSE'].shape)

		df = pd.DataFrame(df, dtype = 'float')
		df = df.head(len(df)-1)

		return df

	def calculate_features(self, df):

		df['WILLR'] = taCalcIndicator(df, 'WILLR', window = 30)
		df['WILLR_D1'] = df['WILLR'].diff()

		df['RSI'] = taCalcIndicator(df, 'RSI', window = 30)
		df['RSI_D1'] = df['RSI'].diff()
	

		df['CCI'] = taCalcIndicator(df, 'CCI', window = 30)
		df['CCI_D1'] = df['CCI'].diff()

		df['STOCH_K'], df['STOCH_D'] = taCalcSTOCH(df)		

		df['ADX'] = np.log(taCalcIndicator(df, 'ADX', window = 30))
		df['sigma'] = df['CLOSE'].rolling(window = 30, center = False).std()
		df['ATR'] = np.log(taCalcIndicator(df, 'ATR', window = 30))

		df['BP10'] = breakawayEvent(df, window =10)
		df['BP15'] = breakawayEvent(df, window =15)
		df['BP30'] = breakawayEvent(df, window =30)
		df['BP60'] = breakawayEvent(df, window =60)

		df = pd.concat([df, width_metric(ribbon_sma(df))], axis=1)
		df = pd.concat([df, distance_metric(ribbon_sma(df))], axis=1)

		return df

	def predict(self, X):

		X = copy.deepcopy(X)

		X = X.dropna()
		#X_current = X.iloc[-1]
		X = X.as_matrix()

		if self.scaler is not None:
			X = self.scaler.transform(X)

		ls_px = self.model.predict_proba(X)[:,1] # wtf here
		px = ls_px[-1]
		#print ls_px

		return px

	def tradeable(self, X):

		c1 = self.X['BP10'].iloc[-1] != 0
		c2 = (self.time.hour) >= 9
		c3 = (self.time.hour) < 18
		c4 = self.X['ADX'].iloc[-1] >= 3

		if(c1==True)&(c2==True)&(c3==True)&(c4==True):
			return True
		else:
			return False

	def restart(self):
		self.__init__()

	def set_retry_attempts(self, value):
		self.retry_attempts = value

	def increment_retry_attempts(self):
		self.retry_attempts = self.retry_attempts + 1

class TradeHandler(object):
	"""docstring for TradeHandler"""
	def __init__(self, asset, X, px, balance, threshold_up = 0.59, threshold_down = 0.41, proportion = 0.01):
		self.asset = asset
		self.X = X
		self.px = px
		self.balance = balance

		self.delay = 0
		self.proportion = proportion
		self.stake = 0
		self.min_stake = 0.5
		self.max_stake = 5000
		self.threshold_up = threshold_up
		self.threshold_down = threshold_down
		self.payout = 0
		self.proposal = None
		self.execute = None

	def on_bar(self):

		if(self.is_signal()==True):

			self.calc_stake()
			if self.stake < self.min_stake:
				self.set_stake(self.min_stake)
			if self.stake > self.max_stake:
				self.set_stake(max_stake)

			if(self.px >= self.threshold_up):
				contract_type = 'CALL'
			if(self.px <= self.threshold_down):
				contract_type = 'PUT'

			t = self.calc_expiry()

			proposal = 	{
			        "proposal": 1,
			        "amount": str(self.stake),
			        "basis": "stake",
			        "contract_type": contract_type,
			        "currency": "USD",
			        "date_expiry": t,
			        "symbol": self.asset
					}

			self.proposal = proposal
			self.execute = True
		else:
			self.execute = None

	def is_signal(self):
		if (self.px>=self.threshold_up) or (self.px<=self.threshold_down):
			return True
		else:
			return False

	def calc_expiry(self):

		dt_last_bar = self.X.index[-1]
		dt_last_bar = str(dt_last_bar)

		offset = 60
		delta = 300 + offset
		#print dt_last_bar
		dt_last_bar = dt.datetime.strptime(dt_last_bar, '%Y-%m-%d %H:%M:%S')
		t = dt_last_bar + dt.timedelta(seconds = delta) - dt.datetime(1970,1,1)
		#print t
		t = int(t.total_seconds())
		# print dt_last_bar
		#print t
		#print(pd.to_datetime(t, unit = 's'))

		return t
		
	def calc_delay(self):
		return 0

	def set_proposal(self):
		return proposal

	def set_action(self, action):
		self.action = action

	def set_proportion(self, p):
		self.proportion = float(p)

	def set_stake(self, value):
		self.stake = value

	def set_min_stake(self, value):
		self.min_stake = value

	def set_max_stake(self, value):
		self.max_stake = value

	def calc_stake(self):
		stake = self.proportion*self.balance 
		stake = np.round(stake,2)
		self.stake = stake

class ProposalHandler(object):
	"""docstring for ProposalHandler"""
	def __init__(self, response, min_payout = 0.8, max_delay = 5):
		self.response = response['proposal']
		self.min_payout = min_payout
		self.payout = self.calc_win_percentage()
		self.max_delay = max_delay
		self.delay = self.calc_delay()
		self.execute = self.decide_execution()

		self.trade_amount = self.response['ask_price']
		self.id = self.response['id']

	def calc_win_percentage(self):
		win_payout = float(self.response['payout'])
		stake = float(self.response['ask_price'])
		value = (win_payout-stake)/stake
		#print value
		return value

	def calc_delay(self):
		date_start = float(self.response['date_start'])
		spot_time = float(self.response['spot_time'])
		delay = date_start-spot_time
		print delay
		return delay

	def decide_execution(self):

		c1 = self.payout >= self.min_payout
		c2 = self.delay <= self.max_delay
		c3 = True

		if(c1==True)&(c2==True)&(c3==True):
			return True
		else:
			return False

	def execute_request(self):

		message = {'buy': self.id, 'price': self.trade_amount}

		return message
		



class BalanceLogger(object):
	"""docstring for BalanceLogger"""
	def __init__(self):
		self.current = 0

	def get_balance(self):
		return float(self.current)

	def set_balance(self, value):
		self.current = value
		
