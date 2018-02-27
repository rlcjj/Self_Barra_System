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
import copy

start_date = "20070115"
end_date = "20171231"
Now_Index = "zz500"
descriptor_1_list = ['ETOP', 'Earnings_STG', 'MLEV', 'BLEV', 'DTOA', 'STO_1M', 'STO_3M', 'STO_12M', 'BTOP', 'EP_Fwd12M', 'CashFlowYield_TTM', 'ROE', 'YOY_Profit']
descriptor_2_list = ['LNCAP', 'Long_Momentum', 'Medium_Momentum', 'Short_Momentum', 'DASTD', 'CMRA']
descriptor_3_list = ['Beta', 'HSIGMA', 'NLSIZE']
factor_list = ['Size', 'Beta', 'Momentum', 'Volatility', 'NL_Size', 'Book_to_Price', 'Liquidity', 'Earnings', 'Growth', 'Leverage', 'ROR']
factor_list1 = ['Earnings', 'Growth', 'Book_to_Price', 'Leverage', 'Liquidity']
factor_list2 = ['Beta', 'Momentum', 'Size', 'Volatility', 'NL_Size']

#'''
#***获取成分股数据***
#'''
#components_dict = db_interaction.get_data_commonly("daily_index_components_" + Now_Index, ["stock_id"], ["curr_date"], 1, 0, 0)
#for date in components_dict.keys():
#    components_dict[date] = xyk_common_data_processing.change_stock_format("no_tail", "with_tail", components_dict[date], 1)
#daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
#
#'''
#***获得全部股票代码序列，用于查询行情和描述量值***
#'''
#a_stock_no_st_dict = db_data_pre_treat.get_a_stock_no_st_dict(start_date, end_date, 0, 6, 1, 0)
#total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_no_st_dict)

'''
***从描述量数据中查询我们需要的部分，从行情序列中同样进行查询***
'''
#descriptors_1_dict = db_interaction.get_daily_data_dict(start_date, end_date, "daily_stock_descriptors_pretreated", descriptor_1_list, total_stock_list, 0)
descriptors_2_dict = db_interaction.get_daily_data_dict(start_date, end_date, "daily_stock_descriptors_unified_pretreated", descriptor_2_list, total_stock_list, 0)
descriptors_3_dict = db_interaction.get_daily_data_dict(start_date, end_date, "daily_stock_descriptors_" + Now_Index + "_unique_pretreated", descriptor_3_list, total_stock_list, 0)
#hq_dict = db_interaction.get_daily_data_dict(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close'], total_stock_list, 0)

'''
***将标准化后的描述量聚合成因子***
'''
def factor_aggregation(whole_standardized_descriptor_list, index = 0):
    factor_list = []
    if index == 0:
        for stock_value in whole_standardized_descriptor_list:
            earnings = xyk_common_data_processing.weighted_mean([stock_value[0], stock_value[9], stock_value[10]], [0.11, 0.68, 0.21], 2)
            growth = xyk_common_data_processing.weighted_mean([stock_value[1], stock_value[11], stock_value[12]], [0.1, 0.5, 0.4], 2)
            BP = stock_value[8]
            leverage = xyk_common_data_processing.weighted_mean([stock_value[2], stock_value[3], stock_value[4]], [0.38, 0.27, 0.35], 2)
            liquidity = xyk_common_data_processing.weighted_mean([stock_value[5], stock_value[6], stock_value[7]], [0.35, 0.35, 0.30], 2)
            stock_factor_list = [earnings, growth, BP, leverage, liquidity]
            factor_list.append(stock_factor_list)
    elif index == 1:
        for stock_value in whole_standardized_descriptor_list:
            beta = stock_value[6]
            momentum = xyk_common_data_processing.weighted_mean([stock_value[1], stock_value[2], stock_value[3]], [0.25, 0.25, 0.5], 2)
            size = stock_value[0]
            volatility = xyk_common_data_processing.weighted_mean([stock_value[4], stock_value[5], stock_value[7]], [0.74, 0.16, 0.10], 2)
            non_linear = stock_value[8]
            stock_factor_list = [beta, momentum, size, volatility, non_linear]
            factor_list.append(stock_factor_list)
    return factor_list

'''
***根据Index篮子确定标准化的参数***
'''
para_dict = {}
para_dict2 = {}
for ord_date, date in enumerate(daily_date_list):
    if ord_date == len(daily_date_list) - 1:
        pass
    else:
        print ord_date, "0"
        this_stock_liquid_list = []
        daily_des_value_list = []
        for stock in components_dict[date]:
            this_stock_daily_des_value_list = []
#            if descriptors_1_dict.has_key((stock, date)) == True:
#                this_stock_daily_des_value_list += descriptors_1_dict[(stock, date)]
            if descriptors_2_dict.has_key((stock, date)) == True:
                this_stock_daily_des_value_list += descriptors_2_dict[(stock, date)]
            if descriptors_3_dict.has_key((stock, date)) == True:
                this_stock_daily_des_value_list += descriptors_3_dict[(stock, date)]
            null_count = 0
            for value in this_stock_daily_des_value_list:
                if value == "" or value == None:
                    null_count += 1
            if null_count < 4:
                if hq_dict.has_key((stock, date)) == True:
                    this_stock_liquid_list.append(hq_dict[(stock, date)][0])
                    daily_des_value_list.append(copy.deepcopy(this_stock_daily_des_value_list))
        daily_des_value_list_T = xyk_common_data_processing.exchange_sequence(daily_des_value_list)
        daily_des_value_list_T_standard = []
        i = 0
        while i < len(daily_des_value_list_T):
            para_dict[(date, i)] = [xyk_common_data_processing.weighted_mean(daily_des_value_list_T[i], this_stock_liquid_list, 2), xyk_common_data_processing.dev_n(daily_des_value_list_T[i])]
            temp_list = xyk_common_data_processing.element_cal_between_list(daily_des_value_list_T[i], para_dict[(date, i)][0], "-")
            temp_list2 = xyk_common_data_processing.element_cal_between_list(temp_list, para_dict[(date, i)][1], "/")
            daily_des_value_list_T_standard.append(temp_list2)
            i += 1
        daily_des_value_list_standard = xyk_common_data_processing.exchange_sequence(daily_des_value_list_T_standard)
        factor_list = factor_aggregation(daily_des_value_list_standard, 1)
        daily_factor_list_T = xyk_common_data_processing.exchange_sequence(factor_list)
        i = 0
        while i < len(daily_factor_list_T):
            para_dict2[(date, i)] = [xyk_common_data_processing.weighted_mean(daily_factor_list_T[i], this_stock_liquid_list, 2), xyk_common_data_processing.dev_n(daily_factor_list_T[i])]
            i += 1

'''
***所有股票标准化、聚合、再标准化***
'''
for ord_date, date in enumerate(daily_date_list):
    if ord_date == len(daily_date_list) - 1:
        pass
    else:
        print ord_date
        if ord_date >= 0:
            next_date = daily_date_list[ord_date + 1]
            this_stock_list = a_stock_no_st_dict[date]
            this_stock_id_list = []
            this_stock_sqrt_liquid_list = []
            this_stock_ROR_list = []
            daily_des_value_list = []
            for stock in this_stock_list:
                this_stock_daily_des_value_list = []
#                if descriptors_1_dict.has_key((stock, date)) == True:
#                    this_stock_daily_des_value_list += descriptors_1_dict[(stock, date)]
                if descriptors_2_dict.has_key((stock, date)) == True:
                    this_stock_daily_des_value_list += descriptors_2_dict[(stock, date)]
                if descriptors_3_dict.has_key((stock, date)) == True:
                    this_stock_daily_des_value_list += descriptors_3_dict[(stock, date)]
                null_count = 0
                for value in this_stock_daily_des_value_list:
                    if value == "" or value == None:
                        null_count += 1
                if null_count < 4:
                    if hq_dict.has_key((stock, date)) == True and hq_dict.has_key((stock, next_date)) == True:
                        daily_des_value_list.append(this_stock_daily_des_value_list)
                        this_stock_sqrt_liquid_list.append(math.sqrt(hq_dict[(stock, date)][0]))
                        this_stock_ROR_list.append(hq_dict[(stock, next_date)][1] / hq_dict[(stock, date)][1] - 1.0)
                        this_stock_id_list.append(stock)
            daily_des_value_list_T = xyk_common_data_processing.exchange_sequence(daily_des_value_list)
            daily_des_value_list_T_standard = []
            i = 0
            while i < len(daily_des_value_list_T):
                temp_list = xyk_common_data_processing.element_cal_between_list(daily_des_value_list_T[i], para_dict[(date, i)][0], "-")
                temp_list2 = xyk_common_data_processing.element_cal_between_list(temp_list, para_dict[(date, i)][1], "/")
                daily_des_value_list_T_standard.append(temp_list2)
                i += 1
            daily_des_value_list_standard = xyk_common_data_processing.exchange_sequence(daily_des_value_list_T_standard)
            factor_list = factor_aggregation(daily_des_value_list_standard, 1)
            daily_factor_list_T = xyk_common_data_processing.exchange_sequence(factor_list)
            daily_factor_list_T_standard = []
            i = 0
            while i < len(daily_factor_list_T):
                temp_list = xyk_common_data_processing.element_cal_between_list(daily_factor_list_T[i], para_dict2[(date, i)][0], "-")
                temp_list2 = xyk_common_data_processing.element_cal_between_list(temp_list, para_dict2[(date, i)][1], "/")
                daily_factor_list_T_standard.append(temp_list2)
                i += 1
            this_date = [date] * len(this_stock_id_list)
            daily_factor_list_T_standard += [this_date]
            daily_factor_list_T_standard += [this_stock_id_list]
            daily_factor_list_T_standard += [this_stock_ROR_list]
            daily_factor_list_T_standard += [this_stock_sqrt_liquid_list]
            daily_factor_list_standard = xyk_common_data_processing.exchange_sequence(daily_factor_list_T_standard)
            i = 0
            while i < len(daily_factor_list_standard):
                j = 0
                while j < len(daily_factor_list_standard[i]):
                    if daily_factor_list_standard[i][j] == None:
                        daily_factor_list_standard[i][j] = "NULL"
                    elif isinstance(daily_factor_list_standard[i][j], str):
                        pass
                    elif math.isnan(daily_factor_list_standard[i][j]) == True:
                        daily_factor_list_standard[i][j] = "NULL"
                    j += 1
                i += 1
            db_interaction.insert_attributes_commonly("daily_stock_factors_" + Now_Index, daily_factor_list_standard, factor_list2 + ["curr_date", "stock_id"] + ["ROR", "sqrt_liquid"], factor_list2 + ["ROR", "sqrt_liquid"])
        