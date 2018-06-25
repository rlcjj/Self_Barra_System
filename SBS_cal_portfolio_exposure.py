# -*- coding: utf-8 -*-
"""
Created on Thu May 31 10:16:11 2018

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
import db_data_pre_treat
import csv

start_date = "20180525"
end_date = "20180525"
factor_list = ['Book_to_Price', 'Earnings', 'Growth', 'Leverage', 'Liquidity', 'Size', 'NL_Size', 'Beta', 'Momentum', 'Residual_Volatility']
Now_Index = "zz800"

'''
***从csv中读取考察的组合的权重***
'''


'''
***读取对应的因子数据***
'''
factor_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_factors_" + Now_Index, factor_list, [], 0, 1, 1)

