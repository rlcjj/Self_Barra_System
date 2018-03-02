# -*- coding: utf-8 -*-
"""
Created on Thu Mar 01 15:05:42 2018

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
Now_Index = "all"

descriptor_1_list = ['ETOP', 'Earnings_STG', 'MLEV', 'BLEV', 'DTOA', 'BTOP', 'EP_Fwd12M', 'CashFlowYield_TTM', 'ROE', 'YOY_Profit']
descriptor_2_list = ['LNCAP', 'Long_Momentum', 'Medium_Momentum', 'Short_Momentum', 'DASTD', 'CMRA']
descriptor_3_list = ['Beta', 'HSIGMA', 'NLSIZE']
descriptor_4_list = ['STO_1M', 'STO_3M', 'STO_12M']
Descriptor_List = [descriptor_1_list, descriptor_2_list, descriptor_3_list, descriptor_4_list]

Source_Table_Name_List = ["daily_stock_descriptors_fundamental", "daily_stock_descriptors_unified", "daily_stock_descriptors_" + Now_Index + "_unique", "daily_stock_descriptors_unified"]

a_stock_normal_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6)
total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_normal_dict)

i = 0
while i < 4:
    if i == 0:
        raw_data_df = db_interaction.get_daily_data_df(start_date, end_date, Source_Table_Name_List[i], Descriptor_List[i], dt_to_str = 1)
    else:
        raw_data_df_temp = db_interaction.get_daily_data_df(start_date, end_date, Source_Table_Name_List[i], Descriptor_List[i], dt_to_str = 1)
        raw_data_df = raw_data_df.merge(raw_data_df_temp, how = 'outer', left_index = True, right_index = True)
    i += 1
    
raw_data_df['STO_1M'] = np.log(raw_data_df['STO_1M'])
raw_data_df['STO_3M'] = np.log(raw_data_df['STO_3M'])
raw_data_df['STO_12M'] = np.log(raw_data_df['STO_12M'])

raw_data_df_temp = db_interaction.get_daily_data_df(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close'], dt_to_str = 1)
raw_data_df = raw_data_df.merge(raw_data_df_temp, how = 'outer', left_index = True, right_index = True)

if Now_Index == "all":
    components_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6)
else:
    where = Now_Index + " = 1"
    components_dict = db_interaction.get_data_commonly("daily_index_components", ["stock_id"], ["curr_date"], one_to_one = 0, where = where)

daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)

all_descriptor_list = descriptor_1_list + descriptor_2_list + descriptor_3_list + descriptor_4_list

bound_dict = {}
for date in daily_date_list:
    print date, 'calculating bounds...'
    this_day_lower_bound_list = []
    this_day_upper_bound_list = []
    temp_mi_list = []
    for stock in components_dict[date]:
        temp_mi_list.append([stock, date])
    temp_mi_df = pd.DataFrame(temp_mi_list, columns = ["stock_id" , "curr_date"])
    this_date_components_df = raw_data_df.loc[raw_data_df.index.intersection(temp_mi_df.set_index(['stock_id','curr_date']).index)]
    for descriptor in all_descriptor_list:
        this_weighted_mean = xyk_common_data_processing.weighted_mean(this_date_components_df.loc[:, descriptor], this_date_components_df.loc[:, 'liquid_MV'], has_null = 2, use_df = 1)
        this_stdev = this_date_components_df.loc[:, descriptor].std(ddof = 1)
        this_des_lower_bound = this_weighted_mean - 3.0 * this_stdev
        this_des_upper_bound = this_weighted_mean + 3.0 * this_stdev
        this_day_lower_bound_list.append(this_des_lower_bound)
        this_day_upper_bound_list.append(this_des_upper_bound)
    bound_dict[date] = [this_day_lower_bound_list, this_day_upper_bound_list]
    
for date in daily_date_list:
    print date, 'clipping...'
    raw_data_df.loc[:, date] = raw_data_df.loc[:, date].clip(bound_dict[date][0] + [-100.0, -100.0], bound_dict[date][1] + [10000000000.0, 10000000.0], axis = 1)
    
ms_dict = {}
for date in daily_date_list:
    print date, 'calculating cap-mean and stdev...'
    this_day_mean_list = []
    this_day_stdev_list = []
    temp_mi_list = []
    for stock in components_dict[date]:
        temp_mi_list.append([stock, date])
    temp_mi_df = pd.DataFrame(temp_mi_list, columns = ["stock_id" , "curr_date"])
    this_date_components_df = raw_data_df.loc[raw_data_df.index.intersection(temp_mi_df.set_index(['stock_id','curr_date']).index)]
    for descriptor in all_descriptor_list:
        this_weighted_mean = xyk_common_data_processing.weighted_mean(this_date_components_df.loc[:, descriptor], this_date_components_df.loc[:, 'liquid_MV'], has_null = 2, use_df = 1)
        this_stdev = this_date_components_df.loc[:, descriptor].std(ddof = 1)
        this_day_mean_list.append(this_weighted_mean)
        this_day_stdev_list.append(this_stdev)
    ms_dict[date] = [this_day_mean_list, this_day_stdev_list]

for date in daily_date_list:
    print date, 'normalizing...'
    raw_data_df.loc[:, date] = raw_data_df.loc[:, date].sub(ms_dict[date][0] + [0.0, 0.0], axis = 1)
    raw_data_df.loc[:, date] = raw_data_df.loc[:, date].div(ms_dict[date][1] + [1.0, 1.0], axis = 1)
    
print "Begin inserting..."
table_name = "daily_stock_descriptors_pretreated_" + Now_Index
db_interaction.insert_df_afresh(table_name, raw_data_df, index_name_list = ["stock_id", "curr_date"])
    
