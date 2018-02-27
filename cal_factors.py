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

start_date = "20050101"
cal_start_date = "20070115"
end_date = "20171231"

Now_Factor = "DASTD"

#cal_daily_date_list = xyk_common_wind_db_interaction.get_calendar(cal_start_date, end_date, 0)
#daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
#SHIBOR_dict = xyk_common_wind_db_interaction.get_IBOR_dict("SHIBOR", "SHIBORON.IR")
#
#'''
#***首先选出计算的股票篮子，然后从行情数据中查询我们需要的部分***
#'''
#a_stock_no_st_dict = db_data_pre_treat.get_a_stock_no_st_dict(start_date, end_date, 0, 6, 1, 0)
#total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_no_st_dict)
#hq_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close'], total_stock_list, 0)
#
#stock_suspension_dict = db_data_pre_treat.get_stock_suspension_dict_by_stock(total_stock_list, start_date, end_date, sus_type = 1)
#hq_dict_no_suspension = xyk_common_data_processing.get_dict_difference(hq_dict, stock_suspension_dict, list_order = 0)
#
#LN_SHIBOR_dict = {}
#for date in SHIBOR_dict.keys():
#    LN_SHIBOR_dict[date] = math.log(1.0 + SHIBOR_dict[date])

#'''
#***以下为计算LNCAP描述量***
#***上市后有120个可交易日以上时才进行计算***
#'''
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
#                    result_list.append([stock, data[0], math.log(data[1])])
         
#'''
#***以下为计算Momentum的3个描述量***
#***上市后有120个可交易日以上时才进行计算Short，有220个可交易日以上时才开始计算Medium，有340个可交易日以上时才开始计算Long***
#***计算Short时使用20个交易日，计算Medium时使用120个交易日，计算Long时使用21-240个交易日；半衰期均为126***
#'''
#half_life_list = xyk_common_data_processing.get_half_life_list(20, 126)
#result_list = []
#count = 0
#for stock in hq_dict_no_suspension.keys():
#    print count
#    count += 1
#    if len(hq_dict_no_suspension[stock]) > 120:
#        for i, data in enumerate(hq_dict_no_suspension[stock]):
#            if data[0] >= cal_daily_date_list[0] and i >= 120:
#                temp_ln_ROR_list = []
#                this_ln_shibor_list = []
#                j = 0
#                while j < 20:
#                    temp_ln_ROR_list.append(math.log(hq_dict_no_suspension[stock][i - j][2] / hq_dict_no_suspension[stock][i - j - 1][2]))
#                    if LN_SHIBOR_dict.has_key(hq_dict_no_suspension[stock][i - j][0]) == True:
#                        this_ln_shibor_list.append(LN_SHIBOR_dict[hq_dict_no_suspension[stock][i - j][0]])
#                    else:
#                        this_ln_shibor_list.append(LN_SHIBOR_dict['20061008'])
#                    j += 1
#                this_minus_list = xyk_common_data_processing.plus_and_minus_between_list(temp_ln_ROR_list, this_ln_shibor_list, "minus")
#                this_short_momentum_value = xyk_common_data_processing.weighted_mean(this_minus_list, half_life_list)
#                result_list.append([stock, data[0], this_short_momentum_value])
                
#half_life_list = xyk_common_data_processing.get_half_life_list(120, 126)
#result_list = []
#count = 0
#for stock in hq_dict_no_suspension.keys():
#    print count
#    count += 1
#    if len(hq_dict_no_suspension[stock]) > 220:
#        for i, data in enumerate(hq_dict_no_suspension[stock]):
#            if data[0] >= cal_daily_date_list[0] and i >= 220:
#                temp_ln_ROR_list = []
#                this_ln_shibor_list = []
#                j = 0
#                while j < 120:
#                    temp_ln_ROR_list.append(math.log(hq_dict_no_suspension[stock][i - j][2] / hq_dict_no_suspension[stock][i - j - 1][2]))
#                    if LN_SHIBOR_dict.has_key(hq_dict_no_suspension[stock][i - j][0]) == True:
#                        this_ln_shibor_list.append(LN_SHIBOR_dict[hq_dict_no_suspension[stock][i - j][0]])
#                    else:
#                        this_ln_shibor_list.append(LN_SHIBOR_dict['20061008'])
#                    j += 1
#                this_minus_list = xyk_common_data_processing.plus_and_minus_between_list(temp_ln_ROR_list, this_ln_shibor_list, "minus")
#                this_medium_momentum_value = xyk_common_data_processing.weighted_mean(this_minus_list, half_life_list)
#                result_list.append([stock, data[0], this_medium_momentum_value])
    
#half_life_list = xyk_common_data_processing.get_half_life_list(220, 126)
#result_list = []
#count = 0
#for stock in hq_dict_no_suspension.keys():
#    print count
#    count += 1
#    if len(hq_dict_no_suspension[stock]) > 340:
#        for i, data in enumerate(hq_dict_no_suspension[stock]):
#            if data[0] >= cal_daily_date_list[0] and i >= 340:
#                temp_ln_ROR_list = []
#                this_ln_shibor_list = []
#                j = 20
#                while j < 240:
#                    temp_ln_ROR_list.append(math.log(hq_dict_no_suspension[stock][i - j][2] / hq_dict_no_suspension[stock][i - j - 1][2]))
#                    if LN_SHIBOR_dict.has_key(hq_dict_no_suspension[stock][i - j][0]) == True:
#                        this_ln_shibor_list.append(LN_SHIBOR_dict[hq_dict_no_suspension[stock][i - j][0]])
#                    else:
#                        this_ln_shibor_list.append(LN_SHIBOR_dict['20061008'])
#                    j += 1
#                this_minus_list = xyk_common_data_processing.plus_and_minus_between_list(temp_ln_ROR_list, this_ln_shibor_list, "minus")
#                this_long_momentum_value = xyk_common_data_processing.weighted_mean(this_minus_list, half_life_list)
#                result_list.append([stock, data[0], this_long_momentum_value])
                    
'''
***以下为计算Volatility的2个描述量***
***上市后有372个可交易日以上时才进行计算DASTD和CMRA***
***计算DASTD时使用252个交易日，计算CMRA时使用21*12个交易日；DASTD半衰期均为42***
'''
half_life_list = xyk_common_data_processing.get_half_life_list(252, 42)
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
                j = 0
                while j < 252:
                    temp_ROR_list.append(hq_dict_no_suspension[stock][i - j][2] / hq_dict_no_suspension[stock][i - j - 1][2] - 1.0)
                    if SHIBOR_dict.has_key(hq_dict_no_suspension[stock][i - j][0]) == True:
                        this_shibor_list.append(SHIBOR_dict[hq_dict_no_suspension[stock][i - j][0]])
                    else:
                        this_shibor_list.append(SHIBOR_dict['20061008'])
                    j += 1
                this_minus_list = xyk_common_data_processing.plus_and_minus_between_list(temp_ROR_list, this_shibor_list, "minus")
                this_minus_mean = sum(this_minus_list) / float(len(this_minus_list))
                this_treated_list = []
                for minus_data in this_minus_list:
                    this_treated_list.append((minus_data - this_minus_mean) * (minus_data - this_minus_mean))
                this_DASTD_value = math.sqrt(xyk_common_data_processing.weighted_mean(this_treated_list, half_life_list))
                result_list.append([stock, data[0], this_DASTD_value])

#this_ln_shibor_list = []
#j = 0
#while j < 12:
#    this_ln_shibor_list.append(math.log(1.0 + float(21 * j + 21) * SHIBOR_dict['20061008']))
#    j += 1
#result_list = []
#count = 0
#for stock in hq_dict_no_suspension.keys():
#    print count
#    count += 1
#    if len(hq_dict_no_suspension[stock]) > 372:
#        for i, data in enumerate(hq_dict_no_suspension[stock]):
#            if data[0] >= cal_daily_date_list[0] and i >= 372:
#                temp_from_last_monthly_ROR_list = []
#                j = 0
#                while j < 12:
#                    temp_from_last_monthly_ROR_list.append(math.log(hq_dict_no_suspension[stock][i][2] / hq_dict_no_suspension[stock][i - 21 * j - 21][2]))
#                    j += 1
#                Z_T = xyk_common_data_processing.plus_and_minus_between_list(temp_from_last_monthly_ROR_list, this_ln_shibor_list, "minus")
#                this_CMRA_value = max(Z_T) - min(Z_T)
#                result_list.append([stock, data[0], this_CMRA_value])
                
'''
***输出至DB***
'''
i = 0
while i < (len(result_list) / 50000):
    print "Now is the " + str(i) + "th 50000 data..."
    db_interaction.insert_attributes_commonly("daily_stock_descriptors_unified", result_list[(50000 * i): (50000 * (i + 1))], ["stock_id", "curr_date", Now_Factor], [Now_Factor])
    i += 1
db_interaction.insert_attributes_commonly("daily_stock_descriptors_unified", result_list[(50000 * i):], ["stock_id", "curr_date", Now_Factor], [Now_Factor])