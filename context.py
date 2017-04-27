'''
context.py

@version: 1.0

Manages and processes API data

@author: Glenn Kroegel
@contact: glenn.kroegel@gmail.com

CHANGE LOG - Fixed expiry time added 

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
		
		self.price_info_cols = ['OPEN','HIGH','LOW','CLOSE','VOLUME']
		self.feature_cols = None

		self.model = joblib.load('model.pkl')
		self.scaler = joblib.load('scaler.pkl')
		self.px = None

	def on_bar(self, message):

		self.input = message
		
		dataframe = self.format_input(message)
		self.dataframe = self.calculate_features(dataframe)

		self.feature_cols = [col for col in self.dataframe.columns if col not in self.price_info_cols]
		self.X = self.dataframe[self.feature_cols]

		if self.tradeable(self.X) == True:
			self.px = self.predict(self.X)
		else:
			self.px = 0.5

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
		df['WILLR_D2'] = df['WILLR'].diff(2)
		df['WILLR_D5'] = df['WILLR'].diff(5)

		df['RSI'] = taCalcIndicator(df, 'RSI', window = 30)
		df['RSI_D1'] = df['RSI'].diff()
		df['RSI_D2'] = df['RSI'].diff(2)
		df['RSI_D5'] = df['RSI'].diff(5)

		df['ADX'] = taCalcIndicator(df, 'ADX', window = 14)

		df['BP5'] = breakawayEvent(df, window =5)
		df['BP10'] = breakawayEvent(df, window =10)
		df['BP15'] = breakawayEvent(df, window =15)
		df['BP30'] = breakawayEvent(df, window =30)
		df['BP60'] = breakawayEvent(df, window =60)

		return df

	def predict(self, X):

		X = X.dropna()

		X = X.as_matrix()

		if self.scaler is not None:
			X = self.scaler.transform(X)

		X_current = X[-1,:] # checkl
		px = self.model.predict_proba(X_current)[0,1]

		return px

	def tradeable(self, X):

		return True


class TradeHandler(object):
	"""docstring for TradeHandler"""
	def __init__(self, asset, X, px, balance):
		self.asset = asset
		self.X = X
		self.px = px
		self.balance = balance

		self.delay = 0
		self.proportion = 0.01
		self.stake = 0
		self.min_stake = 0.5
		self.max_stake = 5000
		self.threshold_up = 0.6
		self.threshold_down = 0.4
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

			contract_type = 'CALL' # just for testing
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
			return True # Should be false - true for testing

	def calc_expiry(self):

		dt_last_bar = self.X.index[-1]
		dt_last_bar = str(dt_last_bar)
		print dt_last_bar


		offset = 60
		delta = 300 + offset
		#print dt_last_bar
		dt_last_bar = dt.datetime.strptime(dt_last_bar, '%Y-%m-%d %H:%M:%S')
		t = dt_last_bar + dt.timedelta(seconds = delta) - dt.datetime(1970,1,1)
		#print t
		t = int(t.total_seconds())
		# print dt_last_bar
		print t
		print(pd.to_datetime(t, unit = 's'))

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
		self.response = response
		self.min_payout = min_payout
		self.payout = self.calc_win_percentage()
		self.max_delay = max_delay
		self.delay = self.calc_delay()
		self.execute = self.decide_execution()

	def calc_win_percentage(self):
		win_payout = float(self.response['payout'])
		stake = float(self.response['ask_price'])
		value = (win_payout-stake)/stake
		print value
		return value

	def calc_delay(self):
		date_start = float(self.response['date_start'])
		spot_time = float(self.response['spot_time'])
		delay = spot_time-date_start
		return delay

	def decide_execution(self):

		c1 = self.payout >= self.min_payout
		c2 = True
		c3 = True

		if(c1==True)&(c2==True)&(c3==True):
			return True
		else:
			return False
		



class BalanceLogger(object):
	"""docstring for BalanceLogger"""
	def __init__(self):
		self.current = 0

	def get_balance(self):
		return float(self.current)

	def set_balance(self, value):
		self.current = value
		