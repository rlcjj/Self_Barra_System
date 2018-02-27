# -*- coding: utf-8 -*-
"""
Created on Tue Jan 23 13:17:55 2018

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

start_date = "20070115"
end_date = "20171231"
Now_Index = "hs300"
descriptor_1_list = ['ETOP', 'Earnings_STG', 'MLEV', 'BLEV', 'DTOA', 'STO_1M', 'STO_3M', 'STO_12M', 'BTOP', 'EP_Fwd12M', 'CashFlowYield_TTM', 'ROE', 'YOY_Profit']
descriptor_2_list = ['LNCAP', 'Long_Momentum', 'Medium_Momentum', 'Short_Momentum', 'DASTD', 'CMRA']
descriptor_3_list = ['Beta', 'HSIGMA', 'NLSIZE']
descriptor_4_list = ['STO_1M', 'STO_3M', 'STO_12M']
factor_list = ['Size', 'Beta', 'Momentum', 'Volatility', 'NL_Size', 'Book_to_Price', 'Liquidity', 'Earnings', 'Growth', 'Leverage', 'ROR']
table_name = "daily_stock_descriptors_pretreated"

a_stock_no_st_dict = db_data_pre_treat.get_a_stock_no_st_dict(start_date, end_date, 0, 6, 1, 0)
total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_no_st_dict)

'''
***从描述量数据中查询我们需要的部分，从行情序列中同样进行查询***
'''
#descriptors_1_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_descriptors", descriptor_1_list, total_stock_list, 0, 1)
#descriptors_2_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_descriptors_unified", descriptor_2_list, total_stock_list, 0, 1)
#descriptors_3_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_descriptors_" + Now_Index + "_unique", descriptor_3_list, total_stock_list, 0, 1)
descriptors_4_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_descriptors", descriptor_4_list, total_stock_list, 0, 1)
#

'''
***根据Index篮子确定标准化的参数***
'''
pretreat_list = []
count = 0
for date in descriptors_4_dict.keys():
    count += 1
    print count
    
    daily_des_value_list = xyk_common_data_processing.exchange_sequence(descriptors_4_dict[date])
    i = 1
    while i < len(daily_des_value_list):
        
        j = 0
        while j < len(daily_des_value_list[i]):
            if daily_des_value_list[i][j] != "" and daily_des_value_list[i][j] != None:
                if daily_des_value_list[i][j] < 0.0000001:
                    daily_des_value_list[i][j] = -10.0
                else:
                    daily_des_value_list[i][j] = math.log(daily_des_value_list[i][j])
            j += 1
        daily_des_dev = xyk_common_data_processing.dev_n(daily_des_value_list[i])
        daily_des_mean = xyk_common_data_processing.cal_mean(daily_des_value_list[i])
        j = 0
        while j < len(daily_des_value_list[i]):
            if daily_des_value_list[i][j] != "" and daily_des_value_list[i][j] != None:
                if daily_des_value_list[i][j] > daily_des_mean + 3.0 * daily_des_dev:
                    daily_des_value_list[i][j] = daily_des_mean + 3.0 * daily_des_dev
                elif daily_des_value_list[i][j] < daily_des_mean - 3.0 * daily_des_dev:
                    daily_des_value_list[i][j] = daily_des_mean - 3.0 * daily_des_dev
            j += 1                    
        i += 1
    i = 0
    while i < len(daily_des_value_list[0]):
        temp_list = [daily_des_value_list[0][i], date]
        j = 1
        while j < len(daily_des_value_list):
            if daily_des_value_list[j][i] == "" or daily_des_value_list[j][i] == None:
                temp_list.append("NULL")
            else:
                temp_list.append(daily_des_value_list[j][i])
            j += 1
        pretreat_list.append(temp_list)        
        i += 1
    if count % 10 == 0:
        db_interaction.insert_attributes_commonly(table_name, pretreat_list, ["stock_id", "curr_date"] + descriptor_4_list, descriptor_4_list)
        pretreat_list = []
db_interaction.insert_attributes_commonly(table_name, pretreat_list, ["stock_id", "curr_date"] + descriptor_4_list, descriptor_4_list)
#
#'''
#***标准化、聚合、再标准化***
#'''
#for ord_date, date in enumerate(daily_date_list):
#    if ord_date == len(daily_date_list) - 1:
#        pass
#    else:
#        next_date = daily_date_list[ord_date + 1]
#        this_stock_list = components_dict[date]
#        this_stock_liquid_list = []
#        this_stock_sqrt_liquid_list = []
#        this_stock_ROR_list = []
#        for stock in this_stock_list:
#            this_stock_liquid_list.append(hq_dict[(stock, date)][0])
#            this_stock_sqrt_liquid_list.append(math.sqrt(hq_dict[(stock, date)][0]))
#            this_stock_ROR_list.append(hq_dict[(stock, next_date)][1] / hq_dict[(stock, date)][1] - 1.0)
#        whole_standardized_descriptor_list = []
#        for i, descriptor in enumerate(descriptor_list):
#            this_descriptor_value_list = []
#            for stock in this_stock_list:
#                this_value = descriptors_dict[(stock, date)][i]
#                if this_value == "":
#                    this_value = None
#                this_descriptor_value_list.append(this_value)
#            this_descriptor_standardized_list = xyk_common_data_processing.BARRA_nomalize(this_descriptor_value_list, this_stock_liquid_list)
#            this_descriptor_standardized_list = xyk_common_data_processing.shrinkage_to_3_stdev(this_descriptor_standardized_list)
#            whole_standardized_descriptor_list.append(this_descriptor_standardized_list)
#        whole_raw_factor_list = factor_aggregation(whole_standardized_descriptor_list)
#        whole_standardized_factor_list = []
#        for this_factor_list in whole_raw_factor_list:
#            this_factor_standardized_list = xyk_common_data_processing.BARRA_nomalize(this_descriptor_value_list, this_stock_liquid_list)
#            whole_standardized_factor_list.append(this_factor_standardized_list)
#            
#'''
#***输出到DB中***
#'''
#db_interaction.insert_attributes_commonly(table_name, pretreat_list, ["stock_id", "curr_date"] + descriptor_1_list, descriptor_1_list)