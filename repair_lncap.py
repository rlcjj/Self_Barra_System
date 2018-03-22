# -*- coding: utf-8 -*-
"""
Created on Wed Mar 21 09:07:26 2018

@author: xiyuk
"""

import math
import numpy as np
from datetime import datetime
import datetime as dt
import db_interaction
import xyk_common_data_processing
import xyk_common_wind_db_interaction
import db_data_pre_treat

start_date = "20051010"
cal_start_date = "20070115"
end_date = "20180320"

After_date = 0

cal_daily_date_list = xyk_common_wind_db_interaction.get_calendar(cal_start_date, end_date, 0)
daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
SHIBOR_dict = xyk_common_wind_db_interaction.get_IBOR_dict("SHIBOR", "SHIBORON.IR")

'''
***首先选出计算的股票篮子，然后从行情数据中查询我们需要的部分***
'''
a_stock_no_st_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6)
total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_no_st_dict)
hq_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_technical", ['all_MV'], total_stock_list, 0)

stock_suspension_dict = db_data_pre_treat.get_stock_suspension_dict_by_stock(total_stock_list, start_date, end_date, sus_type = 1)
hq_dict_no_suspension = xyk_common_data_processing.get_dict_difference(hq_dict, stock_suspension_dict, list_order = 0)

LN_SHIBOR_dict = {}
for date in SHIBOR_dict.keys():
    LN_SHIBOR_dict[date] = math.log(1.0 + SHIBOR_dict[date])

'''
***以下为计算LNCAP描述量***
***上市后有120个可交易日以上时才进行计算***
'''
result_list1 = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count, 'LNCAP'
    count += 1
    if len(hq_dict_no_suspension[stock]) > 120:
        this_start_date = hq_dict_no_suspension[stock][120][0]
        for data in hq_dict[stock]:
            if data[0] >= this_start_date and data[0] >= cal_daily_date_list[0]:
                if data[1] != '' and data[1] != None:
                    result_list1.append([stock, data[0], math.log(data[1])])
                    
db_interaction.insert_attributes_commonly("daily_stock_descriptors_unified", result_list1, ["stock_id", "curr_date", "LNCAP"], ["LNCAP"], batch = 50000)