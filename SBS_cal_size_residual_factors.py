# -*- coding: utf-8 -*-
"""
Created on Wed Mar 14 10:53:07 2018

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
import statsmodels.api as sm

#start_date = "20070115"
#end_date = "20171231"
#Now_Index = "all"
#change_factor_list = ['Book_to_Price', 'Earnings', 'Growth', 'Leverage', 'Liquidity', 'Beta', 'Momentum', 'Reversal', 'Residual_Volatility']
#keep_value_list = ['Size', 'NL_Size', 'ROR', 'liquid_MV', 'close']
#
#Factor_Table_Name = 'daily_stock_factors_' + Now_Index
#Output_Table_Name = 'daily_stock_factors_size_residual_'  + Now_Index
#
#daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
#
#'''
#***获取成分股数据***
#'''
#if Now_Index == "all":
#    components_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6)
#else:
#    where = Now_Index + " = 1"
#    components_dict = db_interaction.get_data_commonly("daily_index_components", ["stock_id"], ["curr_date"], one_to_one = 0, where = where)
#
#'''
#***从因子表中获取我们需要的几列数据***
#'''
#descriptor_data_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, Factor_Table_Name, change_factor_list + keep_value_list, date_for_key = 1, to_df = 1)

'''
***根据Index篮子做OLS***
'''
output_data_dict = {}
for date in daily_date_list:
    print date, 'calculating size residual factors...'
    #筛选本日成分股的数据
    temp_mi_df = pd.DataFrame(components_dict[date], columns = ["stock_id"])
    this_date_components_df = descriptor_data_dict[date].loc[descriptor_data_dict[date].index.intersection(temp_mi_df.set_index('stock_id').index)]
    #先写入那些保持不变的量
    output_data_dict[date] = this_date_components_df.loc[:, keep_value_list].copy()
    for factor in change_factor_list:
        this_factor_df = this_date_components_df.loc[:, [factor, 'Size']]
        #去空
        this_factor_df = xyk_common_data_processing.delete_none(this_factor_df)
        #做回归
        size_sr = this_factor_df.loc[:, 'Size']
        this_factor_sr = this_factor_df.loc[:, factor]
        X_np = sm.add_constant(size_sr)
        ols_model = sm.OLS(this_factor_sr, X_np)
        results = ols_model.fit()
        this_factor_np = results.resid
        this_factor_df.loc[:, factor] = this_factor_np
        #储存
        output_data_dict[date] = pd.concat([output_data_dict[date], this_factor_df.loc[:, factor]], axis = 1, join = 'outer')

for i, date in enumerate(daily_date_list):
    print date, 'inserting...'
    temp_insert_data = output_data_dict[date]
    temp_insert_data['curr_date'] = date
    temp_insert_data.index.name = 'stock_id'
    temp_insert_data = temp_insert_data.set_index([temp_insert_data['curr_date'], temp_insert_data.index]).drop(['curr_date'], axis = 1)
    db_interaction.insert_df_append(Output_Table_Name, temp_insert_data, index_name_list = ["curr_date", "stock_id"])