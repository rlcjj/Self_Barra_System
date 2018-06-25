# -*- coding: utf-8 -*-
"""
Created on Thu Mar 22 14:10:45 2018

@author: xiyuk
"""

import csv
import math
import numpy as np
import pandas as pd
from datetime import datetime
import datetime as dt
import db_interaction
import xyk_common_data_processing
import xyk_common_wind_db_interaction

pool_name = "dp_pool9"
start_date = "20000000"

'''
***读入CSV数据***
'''
csv_name = pool_name + ".csv"
csvfile = file(csv_name, 'rb')
reader = csv.reader(csvfile)

code_dict = {}

for line in reader:
    stock_id_int = int(line[0])
    date = str(line[1])
    if code_dict.has_key(date) == False:
        code_dict[date] = [stock_id_int]
    else:
        code_dict[date].append(stock_id_int)

csvfile.close()

'''
***修正股票代码格式***
'''
for date in code_dict.keys():
    code_dict[date] = xyk_common_data_processing.change_stock_format("int", "with_tail", code_dict[date])
    
'''
***生成output list（现在每个调仓日期不一定是月末了，但调仓日期后一交易日才会实施该调仓）***
'''
output_list = []
sorted_date_list = sorted(code_dict.keys())
for tt, date in enumerate(sorted_date_list):
    if date >= start_date:
        if len(sorted_date_list) == (tt + 1):
            csv_next_date = ""
        else:
            csv_next_date = sorted_date_list[tt + 1]
        if date[4:6] == "12":
            next_date = str(int(date[:4]) + 1) + "0131"
        elif date[4:6] in ["09", "10", "11"]:
            next_date = date[:4] + str(int(date[4:6]) + 1) + "31"
        else:
            next_date = date[:5] + str(int(date[4:6]) + 1) + "31"
        if csv_next_date != "":
            next_date = min(next_date, csv_next_date)
        print next_date
        daily_date_list = xyk_common_wind_db_interaction.get_calendar(date, next_date, 0)
        for daily_date in daily_date_list:
            if daily_date != date:
                for stock in code_dict[date]:
                    output_list.append([stock, daily_date, 1])
output_list.sort()
                
'''
***输出到数据库***
'''
table_name = "daily_index_components"
db_interaction.insert_attributes_commonly(table_name, output_list, ['stock_id', 'curr_date', pool_name], [pool_name], batch = 50000)