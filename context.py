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
		df = df.head(len(df_bars)-1)

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

		x['BP5'] = breakawayEvent(x, window =5)
		x['BP10'] = breakawayEvent(x, window =10)
		x['BP15'] = breakawayEvent(x, window =15)
		x['BP30'] = breakawayEvent(x, window =30)
		x['BP60'] = breakawayEvent(x, window =60)

		return df

	def predict(self, X):

		X = X.as_matrix()

		if self.scaler is not None:
			X = self.scaler.transform(X)

		X_current = X[-1,:] # checkl
		px = self.model.predict_proba(X_current)[0,1]

		return px

	def tradeable(self, X):

		return True
		