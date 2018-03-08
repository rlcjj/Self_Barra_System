# -*- coding: utf-8 -*-
"""
Created on Mon Mar 05 10:03:01 2018

@author: xiyuk
"""

import math
import numpy as np
import statsmodels.api as sm
from datetime import datetime
import datetime as dt
import db_interaction
import xyk_common_data_processing
import xyk_common_wind_db_interaction
import db_data_pre_treat

start_date = "20070115"
end_date = "20171231"

daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)

a_stock_normal_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 3)
total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_normal_dict)
hq_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_technical", ['close'], total_stock_list, 0)

stock_suspension_dict = db_data_pre_treat.get_stock_suspension_dict_by_stock(total_stock_list, start_date, end_date, sus_type = 1)
hq_dict_no_suspension = xyk_common_data_processing.get_dict_difference(hq_dict, stock_suspension_dict, list_order = 0)

result_list = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count
    count += 1
    if len(hq_dict_no_suspension[stock]) > 1:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= daily_date_list[0] and i >= 1:
                temp_ROR = hq_dict_no_suspension[stock][i][1] / hq_dict_no_suspension[stock][i - 1][1] - 1.0
                result_list.append([stock, hq_dict_no_suspension[stock][i - 1][0], temp_ROR])

print "all"
table_name = "daily_stock_descriptors_pretreated_all"
db_interaction.insert_attributes_commonly(table_name, result_list, ['stock_id', 'curr_date', 'ROR'], ['ROR'], batch = 100000)

print "hs300"
table_name = "daily_stock_descriptors_pretreated_hs300"
db_interaction.insert_attributes_commonly(table_name, result_list, ['stock_id', 'curr_date', 'ROR'], ['ROR'], batch = 100000)

print "zz500"
table_name = "daily_stock_descriptors_pretreated_zz500"
db_interaction.insert_attributes_commonly(table_name, result_list, ['stock_id', 'curr_date', 'ROR'], ['ROR'], batch = 100000)