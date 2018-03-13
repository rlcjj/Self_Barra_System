# -*- coding: utf-8 -*-
"""
Created on Mon Mar 05 10:52:38 2018

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

start_date = "20070115"
end_date = "20171231"
Now_Index = "zz500"

Descriptor_List = ['ETOP', 'Earnings_STG', 'MLEV', 'BLEV', 'DTOA', 'STO_1M', 'STO_3M', 'STO_12M', 'BTOP', 'EP_Fwd12M',\
                   'CashFlowYield_TTM', 'ROE', 'YOY_Profit', 'LNCAP', 'Long_Momentum', 'Medium_Momentum', 'Short_Momentum',\
                   'DASTD', 'CMRA', 'Beta', 'HSIGMA', 'NLSIZE']

Keep_Data_List = ['liquid_MV', 'close', 'ROR']

Factor_List = ['Size', 'Beta', 'Momentum', 'Reversal', 'Volatility', 'NL_Size', 'Book_to_Price', 'Liquidity', 'Earnings', 'Growth', 'Leverage']

Factor_Weight_Dict = {'Size':[['LNCAP'], [1.0]], 'Beta':[['Beta'], [1.0]], 'Momentum':[['Long_Momentum'], [1.0]], \
                      'Reversal':[['Short_Momentum'], [1.0]], 'Volatility':[['DASTD', 'CMRA', 'HSIGMA'], [0.74, 0.16, 0.10]], \
                      'NL_Size':[['NLSIZE'], [1.0]], 'Book_to_Price':[['BTOP'], [1.0]], 'Liquidity':[['STO_1M', 'STO_3M', 'STO_12M'], [0.35, 0.35, 0.30]], \
                      'Earnings':[['ETOP', 'EP_Fwd12M', 'CashFlowYield_TTM'], [0.11, 0.68, 0.21]], 'Growth':[['Earnings_STG', 'ROE', 'YOY_Profit'], [0.1, 0.5, 0.4]], \
                      'Leverage':[['MLEV', 'DTOA', 'BLEV'], [0.38, 0.35, 0.27]]}

Pretreat_Table_Name = 'daily_stock_descriptors_pretreated_' + Now_Index

Factor_Table_Name = 'daily_stock_factors_' + Now_Index

daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)

if Now_Index == "all":
    components_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6)
else:
    where = Now_Index + " = 1"
    components_dict = db_interaction.get_data_commonly("daily_index_components", ["stock_id"], ["curr_date"], one_to_one = 0, where = where)

descriptor_data_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, Pretreat_Table_Name, Descriptor_List + Keep_Data_List, date_for_key = 1, to_df = 1)

factor_data_dict = {}
for date in daily_date_list:
    print date, 'calculating factor values...'
    factor_data_dict[date] = descriptor_data_dict[date].loc[:, Keep_Data_List]
    for factor in Factor_List:
        if len(Factor_Weight_Dict[factor][0]) > 1:
            temp_data_df = xyk_common_data_processing.descriptors_aggregate_to_factor(descriptor_data_dict[date].loc[:, Factor_Weight_Dict[factor][0]], Factor_Weight_Dict[factor][1])
        else:
            temp_data_df = descriptor_data_dict[date].loc[:, Factor_Weight_Dict[factor][0][0]]
        temp_data_df.columns = pd.Index([factor])
        factor_data_dict[date] = pd.concat([factor_data_dict[date], temp_data_df], axis = 1, join = 'inner')
        
for date in daily_date_list:
    factor_data_dict[date].columns = Keep_Data_List + Factor_List
        
ms_dict = {}
for date in daily_date_list:
    print date, 'calculating cap-mean and stdev...'
    this_day_mean_list = []
    this_day_stdev_list = []
    temp_mi_df = pd.DataFrame(components_dict[date], columns = ["stock_id"])
    this_date_components_df = factor_data_dict[date].loc[factor_data_dict[date].index.intersection(temp_mi_df.set_index('stock_id').index)]
    for factor in Factor_List:
        this_weighted_mean = xyk_common_data_processing.weighted_mean(this_date_components_df.loc[:, factor], this_date_components_df.loc[:, 'liquid_MV'], has_null = 2, use_df = 1)
        this_stdev = this_date_components_df.loc[:, factor].std(ddof = 1)
        this_day_mean_list.append(this_weighted_mean)
        this_day_stdev_list.append(this_stdev)
    ms_dict[date] = [this_day_mean_list, this_day_stdev_list]

for date in daily_date_list:
    print date, 'normalizing...'
    if date == daily_date_list[-1]:
        factor_data_dict[date] = factor_data_dict[date].drop(['ROR'], axis = 1)
        factor_data_dict[date] = factor_data_dict[date].sub([0.0, 0.0] + ms_dict[date][0], axis = 1)
        factor_data_dict[date] = factor_data_dict[date].div([1.0, 1.0] + ms_dict[date][1], axis = 1)
    else:
        factor_data_dict[date] = factor_data_dict[date].sub([0.0, 0.0, 0.0] + ms_dict[date][0], axis = 1)
        factor_data_dict[date] = factor_data_dict[date].div([1.0, 1.0, 1.0] + ms_dict[date][1], axis = 1)
    
print "Begin inserting..."
for i, date in enumerate(daily_date_list):
    print date, 'inserting...'
    temp_insert_data = factor_data_dict[date]
    temp_insert_data['curr_date'] = date
    temp_insert_data.index.name = 'stock_id'
    temp_insert_data = temp_insert_data.set_index([temp_insert_data['curr_date'], temp_insert_data.index]).drop(['curr_date'], axis = 1)
#    if i == 0:
#        raw_data_df = temp_insert_data
#    else:
#        raw_data_df = pd.concat([raw_data_df, temp_insert_data])
    db_interaction.insert_df_append(Factor_Table_Name, temp_insert_data, index_name_list = ["curr_date", "stock_id"])