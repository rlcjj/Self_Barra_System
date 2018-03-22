# -*- coding: utf-8 -*-
"""
Created on Tue Mar 20 21:40:35 2018

@author: xiyuk
"""

import math
import numpy as np
import pandas as pd
from datetime import datetime
import datetime as dt
import db_interaction
import xyk_common_data_processing
import xyk_common_wind_db_interaction
import csv

data = pd.read_table('test.csv', sep=',')

data['StockCode'] = xyk_common_data_processing.change_stock_format("int", "with_tail", data.loc[:, 'StockCode'], 0)

del data['ZX1_Code']

data = data.set_index([data['StockCode'], data['TradeDate']]).drop(['TradeDate', 'StockCode'], axis = 1)

db_interaction.insert_df_append("dp_daily_stock_descriptors_fundamental", data, index_name_list = ["stock_id", "curr_date"])