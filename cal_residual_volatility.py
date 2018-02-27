# -*- coding: utf-8 -*-
"""
Created on Thu Feb 01 10:12:37 2018

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
import statsmodels.api as sm

start_date = "20070115"
end_date = "20171231"
Now_Index = "zz500"
factor_list = ['Size', 'Beta', 'Volatility', 'sqrt_liquid']

'''
***获取成分股数据***
'''
components_dict = db_interaction.get_data_commonly("daily_index_components_" + Now_Index, ["stock_id"], ["curr_date"], 1, 0, 0)
for date in components_dict.keys():
    components_dict[date] = xyk_common_data_processing.change_stock_format("no_tail", "with_tail", components_dict[date], 1)
daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)

'''
***获得全部股票代码序列，用于查询行情和描述量值***
'''
a_stock_no_st_dict = db_data_pre_treat.get_a_stock_no_st_dict(start_date, end_date, 0, 6, 1, 0)
total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_no_st_dict)

'''
***从因子表中获取我们需要的几列数据***
'''
factor_dict = db_interaction.get_daily_data_dict(start_date, end_date, "daily_stock_factors_" + Now_Index, factor_list, total_stock_list, 0)

'''
***根据Index篮子确定OLS的参数和再次标准化的参数***
'''
para_dict = {}
para_dict2 = {}
for ord_date, date in enumerate(daily_date_list):
    if ord_date == len(daily_date_list) - 1:
        pass
    else:
        print ord_date, "0"
        daily_stock_liquid_list = []
        daily_size_list = []
        daily_beta_list = []
        daily_volatility_list = []
        for stock in components_dict[date]:
            if factor_dict.has_key((stock, date)) == True:
                this_stock_daily_factor_list = factor_dict[(stock, date)]
                null_count = 0
                for value in this_stock_daily_factor_list:
                    if value == "" or value == None:
                        null_count += 1
                if null_count == 0:
                    daily_stock_liquid_list.append(factor_dict[(stock, date)][3] * factor_dict[(stock, date)][3])
                    daily_size_list.append(factor_dict[(stock, date)][0])
                    daily_beta_list.append(factor_dict[(stock, date)][1])
                    daily_volatility_list.append(factor_dict[(stock, date)][2])
        X_list = [daily_size_list, daily_beta_list]
        X_np = sm.add_constant(xyk_common_data_processing.exchange_sequence(X_list))
        ols_model = sm.OLS(daily_volatility_list, X_np)
        results = ols_model.fit()
        para_dict[date] = results.params
        daily_residual_volatility_np = results.resid
        para_dict2[date] = [xyk_common_data_processing.weighted_mean(daily_residual_volatility_np, daily_stock_liquid_list, 2), xyk_common_data_processing.dev_n(daily_residual_volatility_np)]

'''
***所有股票计算波动率残差***
'''
result_list = []
for ord_date, date in enumerate(daily_date_list):
    if ord_date == len(daily_date_list) - 1:
        pass
    else:
        print ord_date
        this_date_list = a_stock_no_st_dict[date]
        this_date_id_list = []
        this_date_size_list = []
        this_date_beta_list = []
        this_date_volatility_list = []
        for stock in this_date_list:
            if factor_dict.has_key((stock, date)) == True:
                this_stock_daily_factor_list = factor_dict[(stock, date)]
                null_count = 0
                for value in this_stock_daily_factor_list:
                    if value == "" or value == None:
                        null_count += 1
                if null_count == 0:
                    this_date_id_list.append(stock)
                    this_date_size_list.append(factor_dict[(stock, date)][0])
                    this_date_beta_list.append(factor_dict[(stock, date)][1])
                    this_date_volatility_list.append(factor_dict[(stock, date)][2])
        
        size_temp_list = xyk_common_data_processing.element_cal_between_list(this_date_size_list, para_dict[date][1], "*")
        beta_temp_list = xyk_common_data_processing.element_cal_between_list(this_date_beta_list, para_dict[date][2], "*")
        minus_1_list = xyk_common_data_processing.element_cal_between_list(this_date_volatility_list, para_dict[date][0], "-")
        minus_size_list = xyk_common_data_processing.element_cal_between_list(minus_1_list, size_temp_list, "-")
        residual_list = xyk_common_data_processing.element_cal_between_list(minus_size_list, beta_temp_list, "-")
        resi_temp_list = xyk_common_data_processing.element_cal_between_list(residual_list, para_dict2[date][0], "-")
        resi_standard_list = xyk_common_data_processing.element_cal_between_list(resi_temp_list, para_dict2[date][1], "/")
        
        i = 0
        while i < len(resi_standard_list):
            result_list.append([this_date_id_list[i], date, resi_standard_list[i]])
            i += 1

'''
***输出数据***
'''
i = 0
while i < (len(result_list) / 50000):
    print "Now is the " + str(i) + "th 50000 data..."
    db_interaction.insert_attributes_commonly("daily_stock_factors_" + Now_Index, result_list[(50000 * i): (50000 * (i + 1))], ["stock_id", "curr_date", "Residual_Volatility"], ["Residual_Volatility"])
    i += 1
db_interaction.insert_attributes_commonly("daily_stock_factors_" + Now_Index, result_list[(50000 * i):], ["stock_id", "curr_date", "Residual_Volatility"], ["Residual_Volatility"])