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

data = pd.read_table('data20180330.csv', sep=',')

data['StockCode'] = xyk_common_data_processing.change_stock_format("int", "with_tail", data.loc[:, 'StockCode'], 0)

del data['ZX1_Code']

data = data.set_index([data['StockCode'], data['TradeDate']]).drop(['TradeDate', 'StockCode'], axis = 1)

#db_interaction.insert_df_append("dp_daily_stock_descriptors_fundamental", data, index_name_list = ["stock_id", "curr_date"])

column_list = data.columns.tolist()
data.reset_index(level = 1, inplace = True)
data.reset_index(inplace = True)
temp_insert_data_list = xyk_common_data_processing.change_data_format_with_df("DataFrame", "list of lists", data)
i = 0
while i < len(temp_insert_data_list):
    temp_insert_data_list[i][1] = str(temp_insert_data_list[i][1])
    j = 2
    while j < len(temp_insert_data_list[i]):
        if temp_insert_data_list[i][j] == None:
            temp_insert_data_list[i][j] = "NULL"
        j += 1
    i += 1
temp_insert_data_list.sort()
db_interaction.insert_attributes_commonly("dp_daily_stock_descriptors_fundamental", temp_insert_data_list, ["stock_id", "curr_date"] + column_list, column_list, batch = 10000)