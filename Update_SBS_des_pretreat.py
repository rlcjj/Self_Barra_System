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
import statsmodels.api as sm
import db_interaction
import xyk_common_data_processing
import xyk_common_wind_db_interaction
import db_data_pre_treat

ROR_start_date = "20180330"
start_date = "20180402"
ROR_end_date = "20180330"
end_date = "20180402"
Now_Index_List = ["zz800", "hs300", "zz500", "all", "dp_pool"]
nearest_fundamental_update = "20180330"
second_fundamental_update = "20180323"
next_fundamental_update = "20180404"
has_new = 0

def cal_cap_weighted_return(start_date, end_date, index_name, weighted_type = "liquid"):
    print "---calculating cap weighted returns...---"
    '''
    ***获取成分股数据***
    '''
    if index_name == "all":
        components_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 3)
    else:
        where = index_name + " = 1"
        components_dict = db_interaction.get_data_commonly("daily_index_components", ["stock_id"], ["curr_date"], one_to_one = 0, where = where)
    
    daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
    
    '''
    ***获得全部出现过的股票代码序列，用于查询行情信息***
    '''
    total_stock_list = xyk_common_data_processing.get_all_element_from_dict(components_dict)
    
    '''
    ***从行情序列中查询***
    '''
    hq_dict = db_interaction.get_daily_data_dict(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close'], total_stock_list, 0)
    
    '''
    ***计算加权的return，停牌按收益为0计算***
    '''
    weighted_return_list = []
    for ord_date, date in enumerate(daily_date_list):
        if ord_date == 0:
            pass
        else:
            last_date = daily_date_list[ord_date - 1]
            this_stock_list = components_dict[date]
            this_stock_liquid_list = []
            this_stock_today_ROR_list = []
            for stock in this_stock_list:
                if hq_dict.has_key((stock, date)) == True and hq_dict.has_key((stock, last_date)) == True:
                    if hq_dict[(stock, last_date)][0] != "":
                        this_stock_liquid_list.append(hq_dict[(stock, last_date)][0])
                        this_stock_today_ROR_list.append(hq_dict[(stock, date)][1] / hq_dict[(stock, last_date)][1] - 1.0)
            if weighted_type == "liquid":
                this_day_weighted_ROR = xyk_common_data_processing.weighted_mean(this_stock_today_ROR_list, this_stock_liquid_list)
            else:
                print "This weighted type is unknown!"
            weighted_return_list.append([index_name, date, this_day_weighted_ROR])
            
    '''
    ***输出到DB中***
    '''
    db_interaction.insert_attributes_commonly("daily_index_performance", weighted_return_list, ["index_name", "curr_date", "cap_weighted_return"], ["cap_weighted_return"])

def cal_unified_factors(cal_start_date, end_date, After_date = 0):
    print "---calculating unified factors...---"
    start_date = str(int(cal_start_date) - 20000)
    if start_date < "20051010":
        start_date = "20051010"
    cal_daily_date_list = xyk_common_wind_db_interaction.get_calendar(cal_start_date, end_date, 0)
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
    print 'cal LNCAP...'
    result_list1 = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        #print count, 'LNCAP'
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
    print 'cal short momentum...'
    half_life_list = xyk_common_data_processing.get_half_life_list(20, 126)
    result_list2 = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        #print count, 'short'
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
              
    print 'cal medium momentum...'
    half_life_list = xyk_common_data_processing.get_half_life_list(120, 126)
    result_list3 = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        #print count, 'medium'
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
        
    print 'cal long momentum...'
    half_life_list = xyk_common_data_processing.get_half_life_list(220, 126)
    result_list4 = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        #print count, 'long'
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
    print 'cal DASTD...'
    half_life_list = xyk_common_data_processing.get_half_life_list(252, 42)
    result_list5 = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        #print count, 'DASTD'
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
    
    print 'cal CMRA...'
    this_ln_shibor_list = []
    j = 0
    while j < 12:
        this_ln_shibor_list.append(math.log(1.0 + float(21 * j + 21) * SHIBOR_dict['20061008']))
        j += 1
    result_list6 = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        #print count, 'CMRA'
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
    print 'cal 1M liquidity...'
    result_list7 = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        #print count, '1M'
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
                  
    print 'cal 3M liquidity...'
    result_list8 = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        #print count, '3M'
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
                    
    print 'cal 12M liquidity...'
    result_list9 = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        #print count, '12M'
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

def cal_unique_factors(cal_start_date, end_date, Now_Index, After_date = 0):
    print "---calculating unique factors...---"
    start_date = str(int(cal_start_date) - 20000)
    if start_date < "20051010":
        start_date = "20051010"
    cal_daily_date_list = xyk_common_wind_db_interaction.get_calendar(cal_start_date, end_date, 0)
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
    print "Calculating..."
    half_life_list = xyk_common_data_processing.get_half_life_list(252, 63)
    result_list = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        if len(cal_daily_date_list) > 10:
            print 'unique factors', count
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

def pretreat_no_fundamental(start_date, end_date, Now_Index):
    print "---calculating no fundamental descriptors...---"
    #descriptor_1_list = ['ETOP', 'Earnings_STG', 'MLEV', 'BLEV', 'DTOA', 'BTOP', 'EP_Fwd12M', 'CashFlowYield_TTM']
    descriptor_1_list = ['ROE', 'YOY_Profit']
    descriptor_2_list = ['LNCAP', 'Long_Momentum', 'Medium_Momentum', 'Short_Momentum', 'DASTD', 'CMRA']
    descriptor_3_list = ['Beta', 'HSIGMA']
    descriptor_4_list = ['STO_1M', 'STO_3M', 'STO_12M']
    Descriptor_List = [descriptor_1_list, descriptor_2_list, descriptor_3_list, descriptor_4_list]
    
    Source_Table_Name_List = ["daily_stock_descriptors_fundamental", "daily_stock_descriptors_unified", "daily_stock_descriptors_" + Now_Index + "_unique", "daily_stock_descriptors_unified"]
    
    daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
    
    i = 0
    while i < 5:
        print i, 'building dict...'
        if i == 0:
            raw_data_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, Source_Table_Name_List[i], Descriptor_List[i], date_for_key = 1, to_df = 1)
        elif i == 4:
            raw_data_dict_temp = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close'], date_for_key = 1, to_df = 1)
            for date in raw_data_dict.keys():
                if date in daily_date_list:
                    raw_data_dict[date] = raw_data_dict[date].merge(raw_data_dict_temp[date], how = 'outer', left_index = True, right_index = True)
        else:
            raw_data_dict_temp = db_interaction.get_daily_data_dict_1_key(start_date, end_date, Source_Table_Name_List[i], Descriptor_List[i], date_for_key = 1, to_df = 1)
            for date in raw_data_dict.keys():
                if date in daily_date_list:
                    raw_data_dict[date] = raw_data_dict[date].merge(raw_data_dict_temp[date], how = 'outer', left_index = True, right_index = True)
        i += 1
    
    for date in raw_data_dict.keys():
        if date in daily_date_list:
            raw_data_dict[date]['STO_1M'] = np.log(raw_data_dict[date]['STO_1M'])
            raw_data_dict[date]['STO_3M'] = np.log(raw_data_dict[date]['STO_3M'])
            raw_data_dict[date]['STO_12M'] = np.log(raw_data_dict[date]['STO_12M'])
    
    if Now_Index == "all":
        components_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6)
    else:
        where = Now_Index + " = 1"
        components_dict = db_interaction.get_data_commonly("daily_index_components", ["stock_id"], ["curr_date"], one_to_one = 0, where = where)
    
    all_descriptor_list = descriptor_1_list + descriptor_2_list + descriptor_3_list + descriptor_4_list
    
    #这是使用MAD法来去极值
    bound_dict = {}
    for date in daily_date_list:
        print date, 'calculating bounds...'
        this_day_lower_bound_list = []
        this_day_upper_bound_list = []
        temp_mi_df = pd.DataFrame(components_dict[date], columns = ["stock_id"])
        this_date_components_df = raw_data_dict[date].loc[raw_data_dict[date].index.intersection(temp_mi_df.set_index('stock_id').index)]
        for descriptor in all_descriptor_list:
            this_median = this_date_components_df.loc[:, descriptor].median()
            MAD = (this_date_components_df.loc[:, descriptor] - this_median).abs().median()
            this_des_lower_bound = this_median - 3.0 * 1.483 * MAD
            this_des_upper_bound = this_median + 3.0 * 1.483 * MAD
            this_day_lower_bound_list.append(this_des_lower_bound)
            this_day_upper_bound_list.append(this_des_upper_bound)
        bound_dict[date] = [this_day_lower_bound_list, this_day_upper_bound_list]    
        
    for date in daily_date_list:
        print date, 'clipping...'
        lower_bound = pd.Series(bound_dict[date][0] + [-100.0, -100.0], index = all_descriptor_list + ['liquid_MV', 'close'])
        upper_bound = pd.Series(bound_dict[date][1] + [10000000000.0, 10000000.0], index = all_descriptor_list + ['liquid_MV', 'close'])
        raw_data_dict[date] = raw_data_dict[date].clip(lower_bound, upper_bound, axis = 1)
        
    ms_dict = {}
    for date in daily_date_list:
        print date, 'calculating cap-mean and stdev...'
        this_day_mean_list = []
        this_day_stdev_list = []
        temp_mi_df = pd.DataFrame(components_dict[date], columns = ["stock_id"])
        this_date_components_df = raw_data_dict[date].loc[raw_data_dict[date].index.intersection(temp_mi_df.set_index('stock_id').index)]
        for descriptor in all_descriptor_list:
            this_weighted_mean = xyk_common_data_processing.weighted_mean(this_date_components_df.loc[:, descriptor], this_date_components_df.loc[:, 'liquid_MV'], has_null = 2, use_df = 1)
            this_stdev = this_date_components_df.loc[:, descriptor].std(ddof = 1)
            this_day_mean_list.append(this_weighted_mean)
            this_day_stdev_list.append(this_stdev)
        ms_dict[date] = [this_day_mean_list, this_day_stdev_list]
    
    for date in daily_date_list:
        print date, 'normalizing...'
        raw_data_dict[date] = raw_data_dict[date].sub(ms_dict[date][0] + [0.0, 0.0], axis = 1)
        raw_data_dict[date] = raw_data_dict[date].div(ms_dict[date][1] + [1.0, 1.0], axis = 1)
        
    print "Begin inserting..."
    table_name = "daily_stock_descriptors_pretreated_" + Now_Index
    for d, date in enumerate(daily_date_list):
        print date, 'inserting...'
        temp_insert_data = raw_data_dict[date]
        temp_insert_data['curr_date'] = date
        temp_insert_data.index.name = 'stock_id'
        temp_insert_data = temp_insert_data.set_index([temp_insert_data['curr_date'], temp_insert_data.index]).drop(['curr_date'], axis = 1)
        temp_insert_data.reset_index(level = 0, inplace = True)
        temp_insert_data.reset_index(inplace = True)
        temp_insert_data_list = xyk_common_data_processing.change_data_format_with_df("DataFrame", "list of lists", temp_insert_data)
        i = 0
        while i < len(temp_insert_data_list):
            j = 0
            while j < len(temp_insert_data_list[i]):
                if temp_insert_data_list[i][j] == None:
                    temp_insert_data_list[i][j] = "NULL"
                j += 1
            i += 1
        temp_insert_data_list.sort()
        db_interaction.insert_attributes_commonly(table_name, temp_insert_data_list, ["stock_id", "curr_date"] + all_descriptor_list + ['liquid_MV', 'close'], all_descriptor_list + ['liquid_MV', 'close'], batch = 50000)
    
def pretreat_fundamental(start_date, end_date, Now_Index, next_date = ""):
    print "---calculating fundamental descriptors...---"
    descriptor_1_list = ['ETOP', 'Earnings_STG', 'MLEV', 'BLEV', 'DTOA', 'BTOP', 'EP_Fwd12M', 'CashFlowYield_TTM']
    Descriptor_List = [descriptor_1_list]
    
    Source_Table_Name_List = ["dp_daily_stock_descriptors_fundamental"]
    
    daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
    
    i = 0
    while i < 2:
        print i, 'building dict...'
        if i == 0:
            raw_data_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, Source_Table_Name_List[i], Descriptor_List[i], date_for_key = 1, to_df = 1)
        elif i == 1:
            raw_data_dict_temp = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close'], date_for_key = 1, to_df = 1)
            for date in raw_data_dict.keys():
                if date in daily_date_list:
                    raw_data_dict[date] = raw_data_dict[date].merge(raw_data_dict_temp[date], how = 'outer', left_index = True, right_index = True)
        else:
            raw_data_dict_temp = db_interaction.get_daily_data_dict_1_key(start_date, end_date, Source_Table_Name_List[i], Descriptor_List[i], date_for_key = 1, to_df = 1)
            for date in raw_data_dict.keys():
                if date in daily_date_list:
                    raw_data_dict[date] = raw_data_dict[date].merge(raw_data_dict_temp[date], how = 'outer', left_index = True, right_index = True)
        i += 1
    
    if Now_Index == "all":
        components_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6)
    else:
        where = Now_Index + " = 1"
        components_dict = db_interaction.get_data_commonly("daily_index_components", ["stock_id"], ["curr_date"], one_to_one = 0, where = where)
    
    if next_date != "":
        next_date_list = xyk_common_wind_db_interaction.get_calendar(end_date, next_date, 0)
    else:
        next_date_list = []
    
    all_descriptor_list = descriptor_1_list
    
    #这是使用MAD法来去极值
    bound_dict = {}
    for date in daily_date_list:
        print date, 'calculating bounds...'
        this_day_lower_bound_list = []
        this_day_upper_bound_list = []
        temp_mi_df = pd.DataFrame(components_dict[date], columns = ["stock_id"])
        this_date_components_df = raw_data_dict[date].loc[raw_data_dict[date].index.intersection(temp_mi_df.set_index('stock_id').index)]
        for descriptor in all_descriptor_list:
            this_median = this_date_components_df.loc[:, descriptor].median()
            MAD = (this_date_components_df.loc[:, descriptor] - this_median).abs().median()
            this_des_lower_bound = this_median - 3.0 * 1.483 * MAD
            this_des_upper_bound = this_median + 3.0 * 1.483 * MAD
            this_day_lower_bound_list.append(this_des_lower_bound)
            this_day_upper_bound_list.append(this_des_upper_bound)
        bound_dict[date] = [this_day_lower_bound_list, this_day_upper_bound_list]    
        
    for date in daily_date_list:
        print date, 'clipping...'
        lower_bound = pd.Series(bound_dict[date][0] + [-100.0, -100.0], index = all_descriptor_list + ['liquid_MV', 'close'])
        upper_bound = pd.Series(bound_dict[date][1] + [10000000000.0, 10000000.0], index = all_descriptor_list + ['liquid_MV', 'close'])
        raw_data_dict[date] = raw_data_dict[date].clip(lower_bound, upper_bound, axis = 1)
        
    ms_dict = {}
    for date in daily_date_list:
        print date, 'calculating cap-mean and stdev...'
        this_day_mean_list = []
        this_day_stdev_list = []
        temp_mi_df = pd.DataFrame(components_dict[date], columns = ["stock_id"])
        this_date_components_df = raw_data_dict[date].loc[raw_data_dict[date].index.intersection(temp_mi_df.set_index('stock_id').index)]
        for descriptor in all_descriptor_list:
            this_weighted_mean = xyk_common_data_processing.weighted_mean(this_date_components_df.loc[:, descriptor], this_date_components_df.loc[:, 'liquid_MV'], has_null = 2, use_df = 1)
            this_stdev = this_date_components_df.loc[:, descriptor].std(ddof = 1)
            this_day_mean_list.append(this_weighted_mean)
            this_day_stdev_list.append(this_stdev)
        ms_dict[date] = [this_day_mean_list, this_day_stdev_list]
    
    for date in daily_date_list:
        print date, 'normalizing...'
        raw_data_dict[date] = raw_data_dict[date].sub(ms_dict[date][0] + [0.0, 0.0], axis = 1)
        raw_data_dict[date] = raw_data_dict[date].div(ms_dict[date][1] + [1.0, 1.0], axis = 1)
        
    print "Begin inserting..."
    table_name = "daily_stock_descriptors_pretreated_" + Now_Index
    for d, date in enumerate(daily_date_list):
        print date, 'inserting...'
        temp_insert_data = raw_data_dict[date]
        temp_insert_data['curr_date'] = date
        temp_insert_data.index.name = 'stock_id'
        temp_insert_data = temp_insert_data.set_index([temp_insert_data['curr_date'], temp_insert_data.index]).drop(['curr_date', 'liquid_MV', 'close'], axis = 1)
        temp_insert_data.reset_index(level = 0, inplace = True)
        temp_insert_data.reset_index(inplace = True)
        temp_insert_data_list = xyk_common_data_processing.change_data_format_with_df("DataFrame", "list of lists", temp_insert_data)
        i = 0
        while i < len(temp_insert_data_list):
            j = 0
            while j < len(temp_insert_data_list[i]):
                if temp_insert_data_list[i][j] == None:
                    temp_insert_data_list[i][j] = "NULL"
                j += 1
            i += 1
        temp_insert_data_list.sort()
        db_interaction.insert_attributes_commonly(table_name, temp_insert_data_list, ["stock_id", "curr_date"] + descriptor_1_list, descriptor_1_list, batch = 50000)
        if date == end_date and next_date != "":
            t = 1
            while t < len(next_date_list):
                print next_date_list[t], 'pre-inserting...'
                temp_insert_data['curr_date'] = next_date_list[t]
                temp_insert_data_list = xyk_common_data_processing.change_data_format_with_df("DataFrame", "list of lists", temp_insert_data)
                i = 0
                while i < len(temp_insert_data_list):
                    j = 0
                    while j < len(temp_insert_data_list[i]):
                        if temp_insert_data_list[i][j] == None:
                            temp_insert_data_list[i][j] = "NULL"
                        j += 1
                    i += 1
                db_interaction.insert_attributes_commonly(table_name, temp_insert_data_list, ["stock_id", "curr_date"] + all_descriptor_list, all_descriptor_list, batch = 50000)
                t += 1
                
def cal_ROR_for_pretreat(start_date, end_date, Now_Index):
    print "---calculating ROR for pretreat...---"
    daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
    
    a_stock_normal_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 3)
    total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_normal_dict)
    hq_dict = db_interaction.get_daily_data_dict_1_key(start_date, end_date, "daily_stock_technical", ['close'], total_stock_list, 0)
    
    stock_suspension_dict = db_data_pre_treat.get_stock_suspension_dict_by_stock(total_stock_list, start_date, end_date, sus_type = 1)
    hq_dict_no_suspension = xyk_common_data_processing.get_dict_difference(hq_dict, stock_suspension_dict, list_order = 0)
    
    result_list = []
    count = 0
    for stock in hq_dict_no_suspension.keys():
        #print count
        count += 1
        if len(hq_dict_no_suspension[stock]) > 1:
            for i, data in enumerate(hq_dict_no_suspension[stock]):
                if data[0] >= daily_date_list[0] and i >= 1:
                    temp_ROR = hq_dict_no_suspension[stock][i][1] / hq_dict_no_suspension[stock][i - 1][1] - 1.0
                    result_list.append([stock, hq_dict_no_suspension[stock][i - 1][0], temp_ROR])
    
    print "inserting..."
    table_name = "daily_stock_descriptors_pretreated_" + Now_Index
    db_interaction.insert_attributes_commonly(table_name, result_list, ['stock_id', 'curr_date', 'ROR'], ['ROR'], batch = 50000)

def cal_factors(start_date, end_date, Now_Index):
    print "---calculating factors...---"
    Descriptor_List = ['ETOP', 'Earnings_STG', 'MLEV', 'BLEV', 'DTOA', 'STO_1M', 'STO_3M', 'STO_12M', 'BTOP', 'EP_Fwd12M',\
                   'CashFlowYield_TTM', 'ROE', 'YOY_Profit', 'LNCAP', 'Long_Momentum', 'Medium_Momentum', 'Short_Momentum',\
                   'DASTD', 'CMRA', 'Beta', 'HSIGMA']
    
    Keep_Data_List = ['liquid_MV', 'close', 'ROR']
    
    Factor_List = ['Size', 'Beta', 'Momentum', 'Volatility', 'Book_to_Price', 'Liquidity', 'Earnings', 'Growth', 'Leverage']
    
    Factor_Weight_Dict = {'Size':[['LNCAP'], [1.0]], 'Beta':[['Beta'], [1.0]], 'Momentum':[['Long_Momentum', 'Medium_Momentum', 'Short_Momentum'], [0.25, 0.25, 0.5]], \
                          'Volatility':[['DASTD', 'CMRA', 'HSIGMA'], [0.74, 0.16, 0.10]], \
                          'Book_to_Price':[['BTOP'], [1.0]], 'Liquidity':[['STO_1M', 'STO_3M', 'STO_12M'], [0.35, 0.35, 0.30]], \
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
            factor_data_dict[date]['ROR'] = 'NULL'
            factor_data_dict[date] = factor_data_dict[date].loc[:, Keep_Data_List + Factor_List]
        else:
            factor_data_dict[date] = factor_data_dict[date].sub([0.0, 0.0, 0.0] + ms_dict[date][0], axis = 1)
            factor_data_dict[date] = factor_data_dict[date].div([1.0, 1.0, 1.0] + ms_dict[date][1], axis = 1)
        
    print "Begin inserting..."
    for d, date in enumerate(daily_date_list):
        print date, 'inserting...'
        temp_insert_data = factor_data_dict[date]
        temp_insert_data['curr_date'] = date
        temp_insert_data.index.name = 'stock_id'
        temp_insert_data = temp_insert_data.set_index([temp_insert_data['curr_date'], temp_insert_data.index]).drop(['curr_date'], axis = 1)
        temp_insert_data.reset_index(level = 0, inplace = True)
        temp_insert_data.reset_index(inplace = True)
        temp_insert_data_list = xyk_common_data_processing.change_data_format_with_df("DataFrame", "list of lists", temp_insert_data)
        i = 0
        while i < len(temp_insert_data_list):
            j = 0
            while j < len(temp_insert_data_list[i]):
                if temp_insert_data_list[i][j] == None:
                    temp_insert_data_list[i][j] = "NULL"
                j += 1
            i += 1
        temp_insert_data_list.sort()
        db_interaction.insert_attributes_commonly(Factor_Table_Name, temp_insert_data_list, ["stock_id", "curr_date"] + Keep_Data_List + Factor_List, Keep_Data_List + Factor_List, batch = 50000)

def cal_nl_size(start_date, end_date, Now_Index):
    print "---calculating non-linear size...---"
    
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
    db_interaction.insert_attributes_commonly(Factor_Table_Name, result_list, ['stock_id', 'curr_date', 'NL_Size'], ['NL_Size'], batch = 50000)

def cal_residual_volatility(start_date, end_date, Now_Index):
    print "---calculating residual volatility...---"
    
    factor_list = ['Beta', 'Volatility', 'liquid_MV', 'Size']
    
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
        print date, 'calculating residual volatility values...'
        #筛选本日成分股的数据
        temp_mi_df = pd.DataFrame(components_dict[date], columns = ["stock_id"])
        this_date_components_df = descriptor_data_dict[date].loc[descriptor_data_dict[date].index.intersection(temp_mi_df.set_index('stock_id').index)]
        #去空
        this_date_components_df = xyk_common_data_processing.delete_none(this_date_components_df)
        #做回归
        volatility_sr = this_date_components_df.loc[:, 'Volatility']
        X_sr = this_date_components_df.loc[:, ['Beta', 'Size']]
        X_np = sm.add_constant(X_sr)
        ols_model = sm.OLS(volatility_sr, X_np)
        results = ols_model.fit()
        para_dict[date] = [results.params[0], results.params[1], results.params[2]]
        daily_residual_volatility_np = results.resid
        #储存
        para_dict2[date] = [xyk_common_data_processing.weighted_mean(daily_residual_volatility_np, this_date_components_df.loc[:, 'liquid_MV'], 2, use_df = 1), pd.Series(daily_residual_volatility_np).std(ddof = 1)]
    
    for date in daily_date_list:
        print date, 'normalizing...'
        volatility_sr = descriptor_data_dict[date].loc[:, 'Volatility']
        beta_sr = descriptor_data_dict[date].loc[:, 'Beta']
        size_sr = descriptor_data_dict[date].loc[:, 'Size']
        output_data_dict[date] = (volatility_sr - para_dict[date][2] * size_sr - para_dict[date][1] * beta_sr - para_dict[date][0] - para_dict2[date][0]) /  para_dict2[date][1]
    
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
    db_interaction.insert_attributes_commonly(Factor_Table_Name, result_list, ['stock_id', 'curr_date', 'Residual_Volatility'], ['Residual_Volatility'], batch = 50000)

def WLS(start_date, end_date, Now_Index):
    print "---calculating WLS data...---"
    
    Now_Range = Now_Index
    factor_list = ['Book_to_Price', 'Earnings', 'Growth', 'Leverage', 'Liquidity', 'Size', 'NL_Size', 'Beta', 'Momentum', 'Residual_Volatility']
    extra_data_list = ['ROR', 'liquid_MV']
    citics_code_list = ['b101', 'b102', 'b103', 'b104', 'b105', 'b106', 'b107', 'b108', 'b109',\
                        'b10a', 'b10b', 'b10c', 'b10d', 'b10e', 'b10f', 'b10g', 'b10h', 'b10i',\
                        'b10j', 'b10k', 'b10l', 'b10m', 'b10n', 'b10o', 'b10p', 'b10q', 'b10r',\
                        'b10s', 'b10t']
    '''
    ***获取每日指数成分股和全部A股的代码***
    '''
    if Now_Range == "all":
        components_dict = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6)
    else:
        where = Now_Range + " = 1"
        components_dict = db_interaction.get_data_commonly("daily_index_components", ["stock_id"], ["curr_date"], one_to_one = 0, where = where)
        
    daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
    a_stock_no_st_dict = db_data_pre_treat.get_a_stock_no_st_dict(start_date, end_date, 0, 6, 1, 0)
    total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_no_st_dict)
    
    '''
    ***获取行业数据并构建行业哑变量***
    '''
    citics_data = db_interaction.get_data_commonly("stock_citics_industry", ["entry_date", "remove_date", "citics_code"], ["stock_id"], 0, 0, 0)
    dummy_variable_dict = db_data_pre_treat.get_indus_dummy_variable_dict(citics_code_list)
    
    '''
    ***从因子表中获取我们需要的全部数据***
    '''
    factor_dict = db_interaction.get_daily_data_dict(start_date, end_date, "daily_stock_factors_" + Now_Index, factor_list + extra_data_list, total_stock_list, 0)
    
    '''
    ***WLS***
    '''
    U_list = []
    T_value_list = []
    R_squared = []
    R_squared_adj = []
    f_list = []
    R_squared_barra_list = []
    output_list = []
    for ord_date, date in enumerate(daily_date_list):
        print date
        if ord_date == len(daily_date_list) - 1:
            next_date = date
        else:
            next_date = daily_date_list[ord_date + 1]
        this_date_stock_list = components_dict[date]
        ROR_list = []
        sqrt_liquid_list = []
        this_whole_stock_factors_list = []
        for stock in this_date_stock_list:
            if factor_dict.has_key((stock, date)) == True:
                illegal = 0
                i = 0
                while i < len(factor_dict[(stock, date)]):
                    if factor_dict[(stock, date)][i] == None:
                        illegal = 1
                    i += 1
                if illegal == 0:
                    if abs(factor_dict[(stock, date)][-2]) < 0.000001:  #如果这一天的后一天无收益，那么可能是停牌了，需查看
                        if factor_dict.has_key((stock, next_date)) == False:
                            illegal = 1
                        elif factor_dict[(stock, next_date)][8] == None:
                            illegal = 1
                        else:
                            pass
                    else:
                        pass
                else:
                    pass
                if illegal == 0:
                    ROR_list.append(factor_dict[(stock, date)][-2])
                    sqrt_liquid_list.append(math.sqrt(factor_dict[(stock, date)][-1]))
                    this_stock_dummy_list = dummy_variable_dict[db_data_pre_treat.mate_stock_indus(stock, date, "CITICS", 1, citics_data)]
                    this_stock_factor_list = []
                    i = 0
                    while i < len(factor_list):
                        this_stock_factor_list.append(factor_dict[(stock, date)][i])
                        i += 1
                    i = 0
                    while i < len(this_stock_dummy_list):
                        this_stock_factor_list.append(this_stock_dummy_list[i])
                        i += 1
                    this_whole_stock_factors_list.append(this_stock_factor_list)
                else:
                    pass
      
        X = sm.add_constant(this_whole_stock_factors_list)
        wls_model = sm.WLS(ROR_list, X, weights = sqrt_liquid_list)
        results = wls_model.fit()
        U_list_temp = results.resid  #残差项，也就是特质因子序列
        T_value_list_temp = results.tvalues  #模型对每个因子的t值
        R_squared_temp = results.rsquared  #模型R2值
        R_squared_adj_temp = results.rsquared_adj  #模型调整后的R2值
        f_list_temp = results.params  #模型参数，也就是因子收益率序列
        U_list.append(U_list_temp)
        T_value_list.append(T_value_list_temp)
        R_squared.append(R_squared_temp)
        R_squared_adj.append(R_squared_adj_temp)
        f_list.append(f_list_temp)
        U2_list_temp = xyk_common_data_processing.element_cal_between_list(U_list_temp, U_list_temp, "*")
        WU2_list_temp = xyk_common_data_processing.element_cal_between_list(U2_list_temp, sqrt_liquid_list, "*")
        Y2_list_temp = xyk_common_data_processing.element_cal_between_list(ROR_list, ROR_list, "*")
        WY2_list_temp = xyk_common_data_processing.element_cal_between_list(Y2_list_temp, sqrt_liquid_list, "*")
        R_squared_barra = 1 - sum(WU2_list_temp) / sum(WY2_list_temp)
        R_squared_barra_list.append(R_squared_barra)
        
        output_list.append([Now_Index, Now_Range, "r_square", "1", date, R_squared_temp])
        output_list.append([Now_Index, Now_Range, "r_square_adj", "1", date, R_squared_adj_temp])
        output_list.append([Now_Index, Now_Range, "R_squared_barra", "1", date, R_squared_barra])
        i = 0
        while i < len(factor_list) + len(citics_code_list) + 1:
            if i == 0:
                output_list.append([Now_Index, Now_Range, "t_value", "1", date, T_value_list_temp[i]])
                output_list.append([Now_Index, Now_Range, "f", "1", date, f_list_temp[i]])
            elif i < len(factor_list) + 1:
                if np.isnan(T_value_list_temp[i]) == False:
                    output_list.append([Now_Index, Now_Range, "t_value", factor_list[i - 1], date, T_value_list_temp[i]])
                if np.isnan(f_list_temp[i]) == False:
                    output_list.append([Now_Index, Now_Range, "f", factor_list[i - 1], date, f_list_temp[i]])
            else:
                if np.isnan(T_value_list_temp[i]) == False:
                    output_list.append([Now_Index, Now_Range, "t_value", citics_code_list[i - 1 - len(factor_list)], date, T_value_list_temp[i]])
                if np.isnan(f_list_temp[i]) == False:
                    output_list.append([Now_Index, Now_Range, "f", citics_code_list[i - 1 - len(factor_list)], date, f_list_temp[i]])
            i += 1
    
    table_name = "daily_barra_factor_return"
    db_interaction.insert_attributes_commonly(table_name, output_list, ['data_table', 'data_range', 'type', 'object', 'curr_date', 'value'], ['value'], batch = 50000)

cal_unified_factors(start_date, end_date)
for Now_Index in Now_Index_List:
    print Now_Index
    cal_cap_weighted_return(ROR_start_date, end_date, Now_Index)
    cal_unique_factors(start_date, end_date, Now_Index)
    pretreat_no_fundamental(start_date, end_date, Now_Index)
    print Now_Index
    if has_new == 0:
        if start_date < nearest_fundamental_update:
            pretreat_fundamental(start_date, nearest_fundamental_update, Now_Index, next_date = next_fundamental_update)
        else:
            pass
    else:
        pretreat_fundamental(second_fundamental_update, nearest_fundamental_update, Now_Index, next_date = next_fundamental_update)
    cal_ROR_for_pretreat(ROR_start_date, end_date, Now_Index)
    print Now_Index
    if has_new == 0:
        cal_factors(ROR_start_date, end_date, Now_Index)
        cal_nl_size(ROR_start_date, end_date, Now_Index)
        cal_residual_volatility(ROR_start_date, end_date, Now_Index)
        WLS(ROR_start_date, ROR_end_date, Now_Index)
    else:
        cal_factors(second_fundamental_update, end_date, Now_Index)
        cal_nl_size(second_fundamental_update, end_date, Now_Index)
        cal_residual_volatility(second_fundamental_update, end_date, Now_Index)
        WLS(second_fundamental_update, ROR_end_date, Now_Index)