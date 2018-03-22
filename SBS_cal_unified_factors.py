# -*- coding: utf-8 -*-
"""
Created on Mon Jan 29 09:59:53 2018

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

start_date = "20151010"
cal_start_date = "20180102"
end_date = "20180320"

After_date = 0

cal_daily_date_list = xyk_common_wind_db_interaction.get_calendar(cal_start_date, end_date, 0)
daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
SHIBOR_dict = xyk_common_wind_db_interaction.get_IBOR_dict("SHIBOR", "SHIBORON.IR")

'''
***首先选出计算的股票篮子，然后从行情数据中查询我们需要的部分***
'''
a_stock_no_st_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6)
total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_no_st_dict)
hq_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close', 'trading_amount', 'all_MV'], total_stock_list, 0)

stock_suspension_dict = db_data_pre_treat.get_stock_suspension_dict_by_stock(total_stock_list, start_date, end_date, sus_type = 1)
hq_dict_no_suspension = xyk_common_data_processing.get_dict_difference(hq_dict, stock_suspension_dict, list_order = 0)

LN_SHIBOR_dict = {}
for date in SHIBOR_dict.keys():
    LN_SHIBOR_dict[date] = math.log(1.0 + SHIBOR_dict[date])

'''
***以下为计算LNCAP描述量***
***上市后有120个可交易日以上时才进行计算***
'''
result_list1 = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count, 'LNCAP'
    count += 1
    if len(hq_dict_no_suspension[stock]) > 120:
        this_start_date = hq_dict_no_suspension[stock][120][0]
        for data in hq_dict[stock]:
            if data[0] >= this_start_date and data[0] >= cal_daily_date_list[0]:
                if data[4] != '' and data[4] != None:
                    result_list1.append([stock, data[0], math.log(data[4])])
         
'''
***以下为计算Momentum的3个描述量***
***上市后有After_date个可交易日以上时才开始计算***
***计算Short时使用20个交易日，计算Medium时使用120个交易日，计算Long时使用21-240个交易日；半衰期均为126***
'''
half_life_list = xyk_common_data_processing.get_half_life_list(20, 126)
result_list2 = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count, 'short'
    count += 1
    if len(hq_dict_no_suspension[stock]) > After_date + 20:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= cal_daily_date_list[0] and i >= After_date + 20:
                temp_ln_ROR_list = []
                this_ln_shibor_list = []
                j = 0
                while j < 20:
                    temp_ln_ROR_list.append(math.log(hq_dict_no_suspension[stock][i - j][2] / hq_dict_no_suspension[stock][i - j - 1][2]))
                    if LN_SHIBOR_dict.has_key(hq_dict_no_suspension[stock][i - j][0]) == True:
                        this_ln_shibor_list.append(LN_SHIBOR_dict[hq_dict_no_suspension[stock][i - j][0]])
                    else:
                        this_ln_shibor_list.append(LN_SHIBOR_dict['20061008'])
                    j += 1
                this_minus_list = xyk_common_data_processing.element_cal_between_list(temp_ln_ROR_list, this_ln_shibor_list, "-")
                this_short_momentum_value = xyk_common_data_processing.weighted_mean(this_minus_list, half_life_list, use_df = 1)
                result_list2.append([stock, data[0], this_short_momentum_value])
                
half_life_list = xyk_common_data_processing.get_half_life_list(120, 126)
result_list3 = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count, 'medium'
    count += 1
    if len(hq_dict_no_suspension[stock]) > After_date + 120:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= cal_daily_date_list[0] and i >= After_date + 120:
                temp_ln_ROR_list = []
                this_ln_shibor_list = []
                j = 0
                while j < 120:
                    temp_ln_ROR_list.append(math.log(hq_dict_no_suspension[stock][i - j][2] / hq_dict_no_suspension[stock][i - j - 1][2]))
                    if LN_SHIBOR_dict.has_key(hq_dict_no_suspension[stock][i - j][0]) == True:
                        this_ln_shibor_list.append(LN_SHIBOR_dict[hq_dict_no_suspension[stock][i - j][0]])
                    else:
                        this_ln_shibor_list.append(LN_SHIBOR_dict['20061008'])
                    j += 1
                this_minus_list = xyk_common_data_processing.element_cal_between_list(temp_ln_ROR_list, this_ln_shibor_list, "-")
                this_medium_momentum_value = xyk_common_data_processing.weighted_mean(this_minus_list, half_life_list, use_df = 1)
                result_list3.append([stock, data[0], this_medium_momentum_value])
    
half_life_list = xyk_common_data_processing.get_half_life_list(220, 126)
result_list4 = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count, 'long'
    count += 1
    if len(hq_dict_no_suspension[stock]) > After_date + 240:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= cal_daily_date_list[0] and i >= After_date + 240:
                temp_ln_ROR_list = []
                this_ln_shibor_list = []
                j = 20
                while j < 240:
                    temp_ln_ROR_list.append(math.log(hq_dict_no_suspension[stock][i - j][2] / hq_dict_no_suspension[stock][i - j - 1][2]))
                    if LN_SHIBOR_dict.has_key(hq_dict_no_suspension[stock][i - j][0]) == True:
                        this_ln_shibor_list.append(LN_SHIBOR_dict[hq_dict_no_suspension[stock][i - j][0]])
                    else:
                        this_ln_shibor_list.append(LN_SHIBOR_dict['20061008'])
                    j += 1
                this_minus_list = xyk_common_data_processing.element_cal_between_list(temp_ln_ROR_list, this_ln_shibor_list, "-")
                this_long_momentum_value = xyk_common_data_processing.weighted_mean(this_minus_list, half_life_list, use_df = 1)
                result_list4.append([stock, data[0], this_long_momentum_value])
                    
'''
***以下为计算Volatility的2个描述量***
***上市后有After_date个可交易日以上时才进行计算DASTD和CMRA***
***计算DASTD时使用252个交易日，计算CMRA时使用21*12个交易日；DASTD半衰期均为42***
'''
half_life_list = xyk_common_data_processing.get_half_life_list(252, 42)
result_list5 = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count, 'DASTD'
    count += 1
    if len(hq_dict_no_suspension[stock]) > After_date + 252:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= cal_daily_date_list[0] and i >= After_date + 252:
                temp_ROR_list = []
                this_shibor_list = []
                j = 0
                while j < 252:
                    temp_ROR_list.append(hq_dict_no_suspension[stock][i - j][2] / hq_dict_no_suspension[stock][i - j - 1][2] - 1.0)
                    if SHIBOR_dict.has_key(hq_dict_no_suspension[stock][i - j][0]) == True:
                        this_shibor_list.append(SHIBOR_dict[hq_dict_no_suspension[stock][i - j][0]])
                    else:
                        this_shibor_list.append(SHIBOR_dict['20061008'])
                    j += 1
                this_minus_list = xyk_common_data_processing.element_cal_between_list(temp_ROR_list, this_shibor_list, "-")
                this_minus_mean = sum(this_minus_list) / float(len(this_minus_list))
                this_treated_list = []
                for minus_data in this_minus_list:
                    this_treated_list.append((minus_data - this_minus_mean) * (minus_data - this_minus_mean))
                this_DASTD_value = math.sqrt(xyk_common_data_processing.weighted_mean(this_treated_list, half_life_list, use_df = 1))
                result_list5.append([stock, data[0], this_DASTD_value])

this_ln_shibor_list = []
j = 0
while j < 12:
    this_ln_shibor_list.append(math.log(1.0 + float(21 * j + 21) * SHIBOR_dict['20061008']))
    j += 1
result_list6 = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count, 'CMRA'
    count += 1
    if len(hq_dict_no_suspension[stock]) > After_date + 252:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= cal_daily_date_list[0] and i >= After_date + 252:
                temp_from_last_monthly_ROR_list = []
                j = 0
                while j < 12:
                    temp_from_last_monthly_ROR_list.append(math.log(hq_dict_no_suspension[stock][i][2] / hq_dict_no_suspension[stock][i - 21 * j - 21][2]))
                    j += 1
                Z_T = xyk_common_data_processing.element_cal_between_list(temp_from_last_monthly_ROR_list, this_ln_shibor_list, "-")
                this_CMRA_value = max(Z_T) - min(Z_T)
                result_list6.append([stock, data[0], this_CMRA_value])
                
'''
***以下为计算Liquidity的3个描述量***
***上市后有120个可交易日以上时才进行计算1M，有180个可交易日以上时才开始计算3M，有340个可交易日以上时才开始计算12M***
***计算1M时使用21个交易日，计算3M时使用63个交易日，计算12M时使用252个交易日***
'''
result_list7 = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count, '1M'
    count += 1
    if len(hq_dict_no_suspension[stock]) > After_date + 21:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= cal_daily_date_list[0] and i >= After_date + 21:
                temp_1M_list = []
                j = 0
                while j < 21:
                    if hq_dict_no_suspension[stock][i - j][3] != None and hq_dict_no_suspension[stock][i - j][1] != None:
                        temp_1M_list.append(hq_dict_no_suspension[stock][i - j][3] / hq_dict_no_suspension[stock][i - j][1] / 10.0)
                    j += 1
                this_1M_value = sum(temp_1M_list)
                result_list7.append([stock, data[0], this_1M_value])
                
result_list8 = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count, '3M'
    count += 1
    if len(hq_dict_no_suspension[stock]) > After_date + 63:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= cal_daily_date_list[0] and i >= After_date + 63:
                temp_3M_list = []
                j = 0
                while j < 63:
                    if hq_dict_no_suspension[stock][i - j][3] != None and hq_dict_no_suspension[stock][i - j][1] != None:
                        temp_3M_list.append(hq_dict_no_suspension[stock][i - j][3] / hq_dict_no_suspension[stock][i - j][1] / 10.0)
                    j += 1
                this_3M_value = sum(temp_3M_list)
                result_list8.append([stock, data[0], this_3M_value])
                
result_list9 = []
count = 0
for stock in hq_dict_no_suspension.keys():
    print count, '12M'
    count += 1
    if len(hq_dict_no_suspension[stock]) > After_date + 252:
        for i, data in enumerate(hq_dict_no_suspension[stock]):
            if data[0] >= cal_daily_date_list[0] and i >= After_date + 252:
                temp_12M_list = []
                j = 0
                while j < 252:
                    if hq_dict_no_suspension[stock][i - j][3] != None and hq_dict_no_suspension[stock][i - j][1] != None:
                        temp_12M_list.append(hq_dict_no_suspension[stock][i - j][3] / hq_dict_no_suspension[stock][i - j][1] / 10.0)
                    j += 1
                this_12M_value = sum(temp_12M_list)
                result_list9.append([stock, data[0], this_12M_value])
                
'''
***输出至DB***
'''
print "Merging..."
result1_pd = xyk_common_data_processing.change_data_format_with_df("list of lists", "DataFrame", result_list1, columns_name_list = ["stock_id", "curr_date", "LNCAP"])
result2_pd = xyk_common_data_processing.change_data_format_with_df("list of lists", "DataFrame", result_list2, columns_name_list = ["stock_id", "curr_date", "Short_Momentum"])
result3_pd = xyk_common_data_processing.change_data_format_with_df("list of lists", "DataFrame", result_list3, columns_name_list = ["stock_id", "curr_date", "Medium_Momentum"])
result4_pd = xyk_common_data_processing.change_data_format_with_df("list of lists", "DataFrame", result_list4, columns_name_list = ["stock_id", "curr_date", "Long_Momentum"])
result5_pd = xyk_common_data_processing.change_data_format_with_df("list of lists", "DataFrame", result_list5, columns_name_list = ["stock_id", "curr_date", "DASTD"])
result6_pd = xyk_common_data_processing.change_data_format_with_df("list of lists", "DataFrame", result_list6, columns_name_list = ["stock_id", "curr_date", "CMRA"])
result7_pd = xyk_common_data_processing.change_data_format_with_df("list of lists", "DataFrame", result_list7, columns_name_list = ["stock_id", "curr_date", "STO_1M"])
result8_pd = xyk_common_data_processing.change_data_format_with_df("list of lists", "DataFrame", result_list8, columns_name_list = ["stock_id", "curr_date", "STO_3M"])
result9_pd = xyk_common_data_processing.change_data_format_with_df("list of lists", "DataFrame", result_list9, columns_name_list = ["stock_id", "curr_date", "STO_12M"])

result_pd_merged = result1_pd.merge(result2_pd, how = 'outer', on = ["stock_id", "curr_date"])
result_pd_merged = result_pd_merged.merge(result3_pd, how = 'outer', on = ["stock_id", "curr_date"])
result_pd_merged = result_pd_merged.merge(result4_pd, how = 'outer', on = ["stock_id", "curr_date"])
result_pd_merged = result_pd_merged.merge(result5_pd, how = 'outer', on = ["stock_id", "curr_date"])
result_pd_merged = result_pd_merged.merge(result6_pd, how = 'outer', on = ["stock_id", "curr_date"])
result_pd_merged = result_pd_merged.merge(result7_pd, how = 'outer', on = ["stock_id", "curr_date"])
result_pd_merged = result_pd_merged.merge(result8_pd, how = 'outer', on = ["stock_id", "curr_date"])
result_pd_merged = result_pd_merged.merge(result9_pd, how = 'outer', on = ["stock_id", "curr_date"])

result_pd_merged.set_index(["stock_id", "curr_date"], inplace = True)
print "Begin inserting..."
table_name = "daily_stock_descriptors_unified"
db_interaction.insert_df_append(table_name, result_pd_merged, index_name_list = ["stock_id", "curr_date"])