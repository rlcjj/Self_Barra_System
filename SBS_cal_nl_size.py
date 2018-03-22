# -*- coding: utf-8 -*-
"""
Created on Thu Feb 01 10:12:37 2018

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

start_date = "20070115"
end_date = "20180320"
Now_Index = "zz800"
factor_list = ['Size', 'liquid_MV']

Factor_Table_Name = 'daily_stock_factors_' + Now_Index

daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)

'''
***获取成分股数据***
'''
if Now_Index == "all":
    components_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6)
else:
    where = Now_Index + " = 1"
    components_dict = db_interaction.get_data_commonly("daily_index_components", ["stock_id"], ["curr_date"], one_to_one = 0, where = where)

'''
***从因子表中获取我们需要的几列数据***
'''
descriptor_data_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, Factor_Table_Name, factor_list, date_for_key = 1, to_df = 1)

'''
***根据Index篮子确定OLS的参数和再次标准化的参数***
'''
para_dict = {}
para_dict2 = {}
output_data_dict = {}
for date in daily_date_list:
    print date, 'calculating non-linear size values...'
    #筛选本日成分股的数据
    temp_mi_df = pd.DataFrame(components_dict[date], columns = ["stock_id"])
    this_date_components_df = descriptor_data_dict[date].loc[descriptor_data_dict[date].index.intersection(temp_mi_df.set_index('stock_id').index)]
    #去空
    this_date_components_df = xyk_common_data_processing.delete_none(this_date_components_df)
    #做回归
    size_sr = this_date_components_df.loc[:, 'Size']
    cubed_size_sr = size_sr * size_sr * size_sr
    X_np = sm.add_constant(size_sr)
    rlm_model = sm.RLM(cubed_size_sr, X_np, M = sm.robust.norms.HuberT())
    results = rlm_model.fit()
    para_dict[date] = [results.params[0], results.params[1]]
    daily_nl_size_np = results.resid
    #储存
    para_dict2[date] = [xyk_common_data_processing.weighted_mean(daily_nl_size_np, this_date_components_df.loc[:, 'liquid_MV'], 2, use_df = 1), pd.Series(daily_nl_size_np).std(ddof = 1)]

for date in daily_date_list:
    print date, 'normalizing...'
    size_sr = descriptor_data_dict[date].loc[:, 'Size']
    cubed_size_sr = size_sr * size_sr * size_sr
    output_data_dict[date] = (cubed_size_sr - para_dict[date][1] * size_sr - para_dict[date][0] - para_dict2[date][0]) /  para_dict2[date][1]

print "Begin inserting..."
result_list = []
for i, date in enumerate(daily_date_list):
    print date, 'appending...'
    j = 0
    while j < len(output_data_dict[date]):
        if np.isnan(output_data_dict[date][j]) == False:
            result_list.append([output_data_dict[date].index.values[j], date, output_data_dict[date][j]])
        j += 1
        
print "Sorting..."
result_list.sort()
print "Inserting..."
db_interaction.insert_attributes_commonly(Factor_Table_Name, result_list, ['stock_id', 'curr_date', 'NL_Size'], ['NL_Size'], batch = 100000)
