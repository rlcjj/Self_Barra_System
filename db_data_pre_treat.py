# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 20:40:02 2016

@author: MouHaiMa
"""

import math
import numpy as np
from datetime import datetime
import datetime as dt
import db_interaction
import xyk_common_wind_db_interaction
import xyk_common_data_processing

'''
------目录------
mate_stock_indus(stock_list, date, indus_type, level, indus_data) --- 为某一日的股票匹配对应的行业
get_indus_dummy_variable_dict(indus_code_list) --- 构建行业哑变量
get_a_stock_dict(start_date, end_date, year, month, board = 1) --- 获取某个时间段的某类股票代码表
get_stock_st_dict(start_date, end_date, st_type = 0) --- 获取某个时间段的某类ST数据
get_a_stock_no_st_dict(start_date, end_date, year = 0, month = 6, board = 1, st_type = 0) --- 获取某个时间段的某类股票数据，剔除ST股
get_stock_suspension_dict_by_date(start_date, end_date, sus_type = 1) --- 获取某个时间段的某类停牌数据，以日期作为key
get_stock_suspension_dict_by_stock(stock_list, start_date, end_date, sus_type = 1) --- 获取某个时间段的某类停牌数据，以股票代码作为key
get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6, board = 1, st_type = 0, sus_type = 1) --- 获取某个时间段的剔除停牌与ST与新股的股票代码dict
'''

'''
***这个函数用来为某一日的股票匹配对应的行业，level控制几级行业，indus_type控制行业分类标准***
'''
#citics_data = db_interaction.get_data_commonly("stock_citics_industry", ["entry_date", "remove_date", "citics_code"], ["stock_id"], 0, 0, 0)
def mate_stock_indus(stock, date, indus_type, level, indus_data):
    indus_code = 'NULL'
    if indus_data.has_key(stock) == True:
        for piece_data in indus_data[stock]:
            if (piece_data[1] == None and piece_data[0] <= date) \
            or (piece_data[1] >= date and piece_data[0] <= date):
                indus_code = piece_data[2]
                break
    if indus_code != 'NULL' and indus_type == "CITICS":
        if level == 1:
            indus_temp = indus_code[:4]
        elif level == 2:
            indus_temp = indus_code[:6]
        elif level == 3:
            indus_temp = indus_code[:8]
        else:
            indus_temp = indus_code
    else:
        indus_temp = indus_code
    return indus_temp

#print mate_stock_indus(["000001.SZ", "000007.SZ"], "20120505", "CITICS", 1, citics_data)

'''
***这个函数根据输入的List的顺序构建一个对角阵的向量字典，用于作为行业哑变量***
'''
def get_indus_dummy_variable_dict(indus_code_list):
    dummy_variable_dict = {}
    n = len(indus_code_list)
    eye_mat = np.eye(n)  
    i = 0
    while i < n:
        dummy_variable_dict[indus_code_list[i]] = eye_mat[i][:].tolist()
        i += 1
    dummy_variable_dict["NULL"] = [0.0] * n
    return dummy_variable_dict

'''
***这个函数用来获取某个时间段的某类股票代码表，可调整某板或上市时间满多久***
'''
def get_a_stock_dict(start_date, end_date, year, month, board = 1):
    if board == 0: #主板
        board_list = ["434006000", "434004000"]
    elif board == 1: #全部
        board_list = ["434006000", "434004000", "434003000", "434001000"]
    elif board == 2: #主板+创业板
        board_list = ["434006000", "434004000", "434001000"]
    elif board == 3: #创业板
        board_list = ["434001000"]
    elif board == 4: #主板+中小企业板
        board_list = ["434006000", "434004000", "434003000"]
    elif board == 5: #创业板+中小企业板
        board_list = ["434001000", "434003000"]
    elif board == 6: #主板补全
        board_list = ["434006000"]
    else:
        print "What else do you want?!"
    daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
    natural_date_list = xyk_common_data_processing.get_natural_datelist(start_date, end_date)
    date_index_dict = xyk_common_data_processing.construct_date_hirabiki_dict(daily_date_list, natural_date_list)
    a_stock_list = db_interaction.get_data_list("a_stock", ["stock_id", "list_date", "delist_date", "board"])
    a_stock_dict = {}
    for date in daily_date_list:
        a_stock_dict[date] = []
    i = 0
    while i < len(a_stock_list):
        if str(a_stock_list[i][3]) in board_list:
            if a_stock_list[i][2] == 'NULL' or a_stock_list[i][2] == "":
                this_list_date = str(a_stock_list[i][1])
                this_start_date = xyk_common_data_processing.get_date_delta(this_list_date, year, month, 1)
                if this_start_date < start_date:
                    this_start_date = start_date
                elif this_start_date > end_date:
                    i += 1
                    continue
                if date_index_dict.has_key(this_start_date) == False:
                    this_start_date = this_start_date[:6] + str(int(this_start_date[6:]) - 1)
                    if date_index_dict.has_key(this_start_date) == False:
                        this_start_date = this_start_date[:6] + str(int(this_start_date[6:]) - 1)
                        if date_index_dict.has_key(this_start_date) == False:
                            this_start_date = this_start_date[:6] + str(int(this_start_date[6:]) - 1)
                if this_start_date <= end_date:
                    if this_start_date < start_date:
                        this_start_date = start_date
                    this_start_date_index = date_index_dict[this_start_date][1]
                    j = this_start_date_index
                    while j < len(daily_date_list):
                        a_stock_dict[daily_date_list[j]].append(str(a_stock_list[i][0]))
                        j += 1
                else:
                    pass
            else:
                this_list_date = str(a_stock_list[i][1])
                this_start_date = xyk_common_data_processing.get_date_delta(this_list_date, year, month, 1)
                if this_start_date < start_date:
                    this_start_date = start_date
                elif this_start_date > end_date:
                    i += 1
                    continue
                if date_index_dict.has_key(this_start_date) == False:
                    this_start_date = this_start_date[:6] + str(int(this_start_date[6:]) - 1)
                    if date_index_dict.has_key(this_start_date) == False:
                        this_start_date = this_start_date[:6] + str(int(this_start_date[6:]) - 1)
                        if date_index_dict.has_key(this_start_date) == False:
                            this_start_date = this_start_date[:6] + str(int(this_start_date[6:]) - 1)
                this_end_date = str(a_stock_list[i][2])
                if this_start_date > end_date or this_end_date < start_date or this_start_date > this_end_date:
                    pass
                else:
                    if this_start_date < start_date:
                        this_start_date = start_date
                    if this_end_date > end_date:
                        this_end_date = end_date
                    this_start_date_index = date_index_dict[this_start_date][1]
                    this_end_date_index = date_index_dict[this_end_date][0]
                    j = this_start_date_index
                    while j <= this_end_date_index:
                        a_stock_dict[daily_date_list[j]].append(str(a_stock_list[i][0]))
                        j += 1
        else:
            pass        
        i += 1
    return a_stock_dict

'''
***这个函数用来获取某个时间段的某类ST数据***
'''
def get_stock_st_dict(start_date, end_date, st_type = 0):
    if st_type == 0: #全部
        type_list = ["S", "Z", "P", "L", "X", "T"]
    else:
        print "Not now..."
    daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
    natural_date_list = xyk_common_data_processing.get_natural_datelist(start_date, end_date)
    date_index_dict = xyk_common_data_processing.construct_date_hirabiki_dict(daily_date_list, natural_date_list)
    stock_st_list = db_interaction.get_data_list("stock_st", ["stock_id", "entry_date", "remove_date", "type"])
    stock_st_dict = {}
    for date in daily_date_list:
        stock_st_dict[date] = []
    i = 0
    while i < len(stock_st_list):
        if str(stock_st_list[i][3]) in type_list:
            if stock_st_list[i][2] == 'NULL' or stock_st_list[i][2] == "":
                this_start_date = str(stock_st_list[i][1])
                if this_start_date <= end_date:
                    if this_start_date < start_date:
                        this_start_date = start_date
                    this_start_date_index = date_index_dict[this_start_date][1]
                    j = this_start_date_index
                    while j < len(daily_date_list):
                        stock_st_dict[daily_date_list[j]].append(str(stock_st_list[i][0]))
                        j += 1
                else:
                    pass
            else:
                this_start_date = str(stock_st_list[i][1])
                this_end_date = str(stock_st_list[i][2])
                if this_start_date > end_date or this_end_date < start_date or this_start_date > this_end_date:
                    pass
                else:
                    if this_start_date < start_date:
                        this_start_date = start_date
                    if this_end_date > end_date:
                        this_end_date = end_date
                    this_start_date_index = date_index_dict[this_start_date][1]
                    this_end_date_index = date_index_dict[this_end_date][0]
                    j = this_start_date_index
                    while j <= this_end_date_index:
                        stock_st_dict[daily_date_list[j]].append(str(stock_st_list[i][0]))
                        j += 1
        else:
            pass        
        i += 1
    return stock_st_dict

'''
***这个函数用来获取某个时间段的某类股票数据，剔除ST股，可调整某板或上市时间满多久***
'''
def get_a_stock_no_st_dict(start_date, end_date, year = 0, month = 6, board = 1, st_type = 0):
    a_stock_dict = get_a_stock_dict(start_date, end_date, year, month, board)
    stock_st_dict = get_stock_st_dict(start_date, end_date, st_type)
    a_stock_no_st_dict = {}
    for date in a_stock_dict.keys():
        setA = set(a_stock_dict[date])
        setB = set(stock_st_dict[date])
        onlyInA = setA.difference(setB)
        a_stock_no_st_dict[date] = sorted(list(onlyInA))
    return a_stock_no_st_dict

'''
***这个函数用来获取某个时间段的某类停牌数据，以日期作为key***
'''
def get_stock_suspension_dict_by_date(start_date, end_date, sus_type = 1):
    if sus_type == 0: #全部
        type_list = ["444001000", "444002000", "444003000", "444004000", "444007000", "444016000"]
    elif sus_type == 1: #一天及以上
        type_list = ["444003000", "444016000"]
    elif sus_type == 2: #早盘停牌
        type_list = ["444001000", "444003000", "444016000"]
    elif sus_type == 3: #尾盘停牌
        type_list = ["444002000", "444003000", "444016000"]
    else:
        print "Not now..."
    daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
    stock_sus_list = db_interaction.get_data_list("stock_suspension", ["stock_id", "entry_date", "remove_date", "type"])
    stock_sus_dict = {}
    for date in daily_date_list:
        stock_sus_dict[date] = []
    i = 0
    while i < len(stock_sus_list):
        if str(stock_sus_list[i][3]) in type_list:
            if stock_sus_dict.has_key(str(stock_sus_list[i][1])):
                stock_sus_dict[str(stock_sus_list[i][1])].append(str(stock_sus_list[i][0]))
            else:
                pass
        else:
            pass        
        i += 1
    return stock_sus_dict

'''
***这个函数用来获取某个时间段的某类停牌数据，以股票代码作为key***
'''
def get_stock_suspension_dict_by_stock(stock_list, start_date, end_date, sus_type = 1):
    if sus_type == 0: #全部
        type_list = ["444001000", "444002000", "444003000", "444004000", "444007000", "444016000"]
    elif sus_type == 1: #一天及以上
        type_list = ["444003000", "444016000"]
    elif sus_type == 2: #早盘停牌
        type_list = ["444001000", "444003000", "444016000"]
    elif sus_type == 3: #尾盘停牌
        type_list = ["444002000", "444003000", "444016000"]
    else:
        print "Not now..."
    stock_sus_list = db_interaction.get_data_list("stock_suspension", ["stock_id", "entry_date", "remove_date", "type"])
    stock_sus_dict = {}
    for stock in stock_list:
        stock_sus_dict[stock] = []
    i = 0
    while i < len(stock_sus_list):
        if str(stock_sus_list[i][3]) in type_list:
            if stock_sus_dict.has_key(str(stock_sus_list[i][0])):
                stock_sus_dict[str(stock_sus_list[i][0])].append(str(stock_sus_list[i][1]))
            else:
                pass
        else:
            pass        
        i += 1
    return stock_sus_dict

'''
***这个函数用来获取某个时间段的剔除停牌与ST与新股的股票代码dict，以date作为key***
'''
def get_normal_stocklist_dict(start_date, end_date, year = 0, month = 6, board = 1, st_type = 0, sus_type = 1):
    a_stock_no_st_dict = get_a_stock_no_st_dict(start_date, end_date, year, month, board, st_type)
    stock_sus_dict = get_stock_suspension_dict_by_date(start_date, end_date, sus_type)
    a_normal_stocklist_dict = {}
    for date in a_stock_no_st_dict.keys():
        setA = set(a_stock_no_st_dict[date])
        setB = set(stock_sus_dict[date])
        onlyInA = setA.difference(setB)
        a_normal_stocklist_dict[date] = sorted(list(onlyInA))
    return a_normal_stocklist_dict