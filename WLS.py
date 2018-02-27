# -*- coding: utf-8 -*-
"""
Created on Wed Jan 24 10:47:28 2018

@author: xiyuk
"""

import statsmodels.api as sm
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
end_date = "20171228"
factor_list = ['Book_to_Price', 'Earnings', 'Growth', 'Leverage', 'Liquidity', 'Size', 'NL_Size', 'Beta', 'Momentum', 'Residual_Volatility', 'ROR', 'sqrt_liquid']
citics_code_list = ['b101', 'b102', 'b103', 'b104', 'b105', 'b106', 'b107', 'b108', 'b109',\
                    'b10a', 'b10b', 'b10c', 'b10d', 'b10e', 'b10f', 'b10g', 'b10h', 'b10i',\
                    'b10j', 'b10k', 'b10l', 'b10m', 'b10n', 'b10o', 'b10p', 'b10q', 'b10r',\
                    'b10s', 'b10t']
Now_Index = "zz500"
Now_Range = "500"

#'''
#***获取每日300、500和全部A股的代码***
#'''
#components_dict_300 = db_interaction.get_data_commonly("daily_index_components_hs300", ["stock_id"], ["curr_date"], 1, 0, 0)
#for date in components_dict_300.keys():
#    components_dict_300[date] = xyk_common_data_processing.change_stock_format("no_tail", "with_tail", components_dict_300[date], 1)
#components_dict_500 = db_interaction.get_data_commonly("daily_index_components_zz500", ["stock_id"], ["curr_date"], 1, 0, 0)
#for date in components_dict_500.keys():
#    components_dict_500[date] = xyk_common_data_processing.change_stock_format("no_tail", "with_tail", components_dict_500[date], 1)
#daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
#a_stock_no_st_dict = db_data_pre_treat.get_a_stock_no_st_dict(start_date, end_date, 0, 6, 1, 0)
#total_stock_list = xyk_common_data_processing.get_all_element_from_dict(a_stock_no_st_dict)
#
#'''
#***获取行业数据并构建行业哑变量***
#'''
#citics_data = db_interaction.get_data_commonly("stock_citics_industry", ["entry_date", "remove_date", "citics_code"], ["stock_id"], 0, 0, 0)
#dummy_variable_dict = db_data_pre_treat.get_indus_dummy_variable_dict(citics_code_list)

'''
***从因子表中获取我们需要的全部数据***
'''
factor_dict = db_interaction.get_daily_data_dict(start_date, end_date, "daily_stock_factors_" + Now_Index, factor_list, total_stock_list, 0)

'''
***WLS***
'''
U_list = []
T_value_list = []
R_squared = []
R_squared_adj = []
f_list = []
R_squared_barra_list = []
for ord_date, date in enumerate(daily_date_list):
    print date
    if ord_date == len(daily_date_list) - 1:
        next_date = date
    else:
        next_date = daily_date_list[ord_date + 1]
    if Now_Range == "300":
        this_date_stock_list = components_dict_300[date]
    elif Now_Range == "500":
        this_date_stock_list = components_dict_500[date]
    else:
        this_date_stock_list = a_stock_no_st_dict[date]
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
            if abs(factor_dict[(stock, date)][-2]) < 0.000001:  #如果这一天的后一天无收益，那么可能是停牌了，需查看
                if factor_dict.has_key((stock, next_date)) == False:
                    illegal = 1
                elif factor_dict[(stock, next_date)][8] == None:
                    illegal = 1
                else:
                    pass
            else:
                pass
            if illegal == 0:
                ROR_list.append(factor_dict[(stock, date)][-2])
                sqrt_liquid_list.append(factor_dict[(stock, date)][-1])
                this_stock_dummy_list = dummy_variable_dict[db_data_pre_treat.mate_stock_indus(stock, date, "CITICS", 1, citics_data)]
                this_stock_factor_list = []
                i = 0
                while i < len(factor_list) - 2:
                    this_stock_factor_list.append(factor_dict[(stock, date)][i])
                    i += 1
                i = 0
                while i < len(this_stock_dummy_list):
                    this_stock_factor_list.append(this_stock_dummy_list[i])
                    i += 1
                this_whole_stock_factors_list.append(this_stock_factor_list)
  
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

#U_df = pd.DataFrame(U_list, index = daily_date_list)
#T_value_df = pd.DataFrame(T_value_list, index = daily_date_list, columns = ["1"] + factor_list[:-2] + citics_code_list)
#R_squared_df = pd.DataFrame(R_squared, index = daily_date_list)
#R_squared_adj_df = pd.DataFrame(R_squared_adj, index = daily_date_list)
#f_df = pd.DataFrame(f_list, index = daily_date_list, columns = ["1"] + factor_list[:-2] + citics_code_list)
#writer = pd.ExcelWriter('output' + Now_Index + '.xlsx')
#f_df.to_excel(writer,'Sheet1')
#T_value_df.to_excel(writer,'Sheet2')
#writer.save()