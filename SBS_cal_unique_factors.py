# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 09:51:34 2018

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

start_date = "20051010"
cal_start_date = "20070115"
end_date = "20180320"

Now_Index = "zz800"

After_date = 0

cal_daily_date_list = xyk_common_wind_db_interaction.get_calendar(cal_start_date, end_date, 0)
daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
SHIBOR_dict = xyk_common_wind_db_interaction.get_IBOR_dict("SHIBOR", "SHIBORON.IR")
index_return_dict = db_interaction.get_data_commonly("daily_index_performance", ["cap_weighted_return"], ["index_name", "curr_date"])

'''
***首先选出计算的股票篮子，然后从行情数据中查询我们需要的部分***
'''
a_stock_normal_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 3)
total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_normal_dict)
hq_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close'], total_stock_list, 0)

stock_suspension_dict = db_data_pre_treat.get_stock_suspension_dict_by_stock(total_stock_list, start_date, end_date, sus_type = 1)
hq_dict_no_suspension = xyk_common_data_processing.get_dict_difference(hq_dict, stock_suspension_dict, list_order = 0)

LN_SHIBOR_dict = {}
for date in SHIBOR_dict.keys():
    LN_SHIBOR_dict[date] = math.log(1.0 + SHIBOR_dict[date])

'''
***以下为计算Beta描述量与HSIGMA描述量***
***上市后有After_date个可交易日以上时才进行计算***
***计算二者时使用252个交易日收益率，半衰期均为63***
'''
half_life_list = xyk_common_data_processing.get_half_life_list(252, 63)
result_list = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count
    count += 1
    if len(hq_dict_no_suspension[stock]) > After_date + 252:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= cal_daily_date_list[0] and i >= After_date + 252:
                temp_ROR_list = []
                this_shibor_list = []
                this_index_ROR_list = []
                j = 0
                while j < 252:
                    temp_ROR_list.append(hq_dict_no_suspension[stock][i - j][2] / hq_dict_no_suspension[stock][i - j - 1][2] - 1.0)
                    if SHIBOR_dict.has_key(hq_dict_no_suspension[stock][i - j][0]) == True:
                        this_shibor_list.append(SHIBOR_dict[hq_dict_no_suspension[stock][i - j][0]])
                    else:
                        this_shibor_list.append(SHIBOR_dict['20061008'])
                    this_index_ROR_list.append(index_return_dict[(Now_Index, hq_dict_no_suspension[stock][i - j][0])])
                    j += 1
                this_minus_list = xyk_common_data_processing.element_cal_between_list(temp_ROR_list, this_shibor_list, "-")
                X = sm.add_constant(this_index_ROR_list)
                Y = this_minus_list
                wls_model = sm.WLS(Y, X, weights = half_life_list)
                results = wls_model.fit()
                this_beta = float(results.params[1])
                resid_list = results.resid
                this_resid_mean = sum(resid_list) / float(len(resid_list))
                this_treated_list = []
                for resid_data in resid_list:
                    this_treated_list.append((resid_data - this_resid_mean) * (resid_data - this_resid_mean))
                this_HSIGMA = math.sqrt(xyk_common_data_processing.weighted_mean(this_treated_list, half_life_list, use_df = 1))
                result_list.append([stock, data[0], this_beta, this_HSIGMA])
                    
'''
***输出至DB***
'''
result_pd = xyk_common_data_processing.change_data_format_with_df("list of lists", "DataFrame", result_list, columns_name_list = ["stock_id", "curr_date", "Beta", "HSIGMA"])
result_pd.set_index(["stock_id", "curr_date"], inplace = True)
print "Begin inserting..."
table_name = "daily_stock_descriptors_" + Now_Index + "_unique"
db_interaction.insert_df_append(table_name, result_pd, index_name_list = ["stock_id", "curr_date"])
