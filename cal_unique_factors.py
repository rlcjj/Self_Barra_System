# -*- coding: utf-8 -*-
"""
Created on Tue Jan 30 09:51:34 2018

@author: xiyuk
"""

# -*- coding: utf-8 -*-
"""
Created on Mon Jan 29 09:59:53 2018

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

start_date = "20050101"
cal_start_date = "20070115"
end_date = "20171231"

Now_Factor_list = ["Beta", "HSIGMA"]
Now_Index = "zz500"

cal_daily_date_list = xyk_common_wind_db_interaction.get_calendar(cal_start_date, end_date, 0)
daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
SHIBOR_dict = xyk_common_wind_db_interaction.get_IBOR_dict("SHIBOR", "SHIBORON.IR")
index_return_dict = db_interaction.get_data_commonly("daily_index_performance", ["cap_weighted_return"], ["index_name", "curr_date"])

'''
***首先选出计算的股票篮子，然后从行情数据中查询我们需要的部分***
'''
a_stock_no_st_dict = db_data_pre_treat.get_a_stock_no_st_dict(start_date, end_date, 0, 6, 1, 0)
total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_no_st_dict)
hq_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close'], total_stock_list, 0)

stock_suspension_dict = db_data_pre_treat.get_stock_suspension_dict_by_stock(total_stock_list, start_date, end_date, sus_type = 1)
hq_dict_no_suspension = xyk_common_data_processing.get_dict_difference(hq_dict, stock_suspension_dict, list_order = 0)

LN_SHIBOR_dict = {}
for date in SHIBOR_dict.keys():
    LN_SHIBOR_dict[date] = math.log(1.0 + SHIBOR_dict[date])

'''
***以下为计算Beta描述量与HSIGMA描述量***
***上市后有372个可交易日以上时才进行计算***
***计算二者时使用252个交易日收益率，半衰期均为63***
'''
half_life_list = xyk_common_data_processing.get_half_life_list(252, 63)
result_list = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count
    count += 1
    if len(hq_dict_no_suspension[stock]) > 372:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= cal_daily_date_list[0] and i >= 372:
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
                    this_index_ROR_list.append(index_return_dict[(Now_Index, hq_dict_no_suspension[stock][i - j][0])][0])
                    j += 1
                this_minus_list = xyk_common_data_processing.plus_and_minus_between_list(temp_ROR_list, this_shibor_list, "minus")
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
                this_HSIGMA = math.sqrt(xyk_common_data_processing.weighted_mean(this_treated_list, half_life_list))
                result_list.append([stock, data[0], this_beta, this_HSIGMA])
                
#'''
#***以下为计算以index为范围标准化的Standard_Size描述量与在那之上回归得到的NLSIZE描述量***
#***上市后有120个可交易日以上时才进行计算***
#***分成两步，先计算index的参数，在用这些参数算出全部stock的描述量***
#'''
#size_hq_dict = db_interaction.get_daily_data_dict(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close'], total_stock_list, 0)
#components_dict = db_interaction.get_data_commonly("daily_index_components_" + Now_Index, ["stock_id"], ["curr_date"], 1, 0, 0)
#for date in components_dict.keys():
#    components_dict[date] = xyk_common_data_processing.change_stock_format("no_tail", "with_tail", components_dict[date], 1)
#
#para_dict = {}
#for date in components_dict.keys():
#    print date
#    raw_index_ln_size_list = []
#    for stock in components_dict[date]:
#        if size_hq_dict.has_key((stock, date)) == True and size_hq_dict[(stock, date)][0] != '' and size_hq_dict[(stock, date)][0] != None:
#            raw_index_ln_size_list.append(math.log(size_hq_dict[(stock, date)][0]))
#    para_dict[date] = [np.mean(raw_index_ln_size_list), np.std(raw_index_ln_size_list)]
#    standard_index_ln_size_list = []
#    cubed_index_ln_size_list = []
#    for data in raw_index_ln_size_list:
#        this_standard_data = (data - para_dict[date][0]) / para_dict[date][1]
#        standard_index_ln_size_list.append(this_standard_data)
#        cubed_index_ln_size_list.append(this_standard_data * this_standard_data * this_standard_data)
#    X = sm.add_constant(standard_index_ln_size_list)
#    Y = cubed_index_ln_size_list
#    ols_model = sm.OLS(Y, X)
#    results = ols_model.fit()
#    this_para_0 = float(results.params[0])
#    this_para_1 = float(results.params[1])
#    para_dict[date].append(this_para_0)
#    para_dict[date].append(this_para_1)
#
#result_list = []
#count = 0
#for stock in hq_dict_no_suspension.keys():
#    print count
#    count += 1
#    if len(hq_dict_no_suspension[stock]) > 120:
#        this_start_date = hq_dict_no_suspension[stock][120][0]
#        for data in hq_dict[stock]:
#            if data[0] >= this_start_date and data[0] >= cal_daily_date_list[0]:
#                if data[1] != '' and data[1] != None:
#                    this_raw_ln_size = math.log(data[1])
#                    this_standard_ln_size = (this_raw_ln_size - para_dict[date][0]) / para_dict[date][1]
#                    this_cubed_ln_size = this_standard_ln_size * this_standard_ln_size * this_standard_ln_size
#                    this_nlsize = this_cubed_ln_size - para_dict[date][2] - para_dict[date][3] * this_standard_ln_size
#                    result_list.append([stock, data[0], this_standard_ln_size, this_nlsize])
                    

'''
***输出至DB***
'''
i = 0
while i < (len(result_list) / 50000):
    print "Now is the " + str(i) + "th 50000 data..."
    db_interaction.insert_attributes_commonly("daily_stock_descriptors_" + Now_Index + "_unique", result_list[(50000 * i): (50000 * (i + 1))], ["stock_id", "curr_date"] + Now_Factor_list, Now_Factor_list)
    i += 1
db_interaction.insert_attributes_commonly("daily_stock_descriptors_" + Now_Index + "_unique", result_list[(50000 * i):], ["stock_id", "curr_date"] + Now_Factor_list, Now_Factor_list)