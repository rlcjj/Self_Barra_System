# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 20:40:02 2016

@author: MouHaiMa
"""

import math
import numpy as np
import pandas as pd
from datetime import datetime
import datetime as dt

'''
------目录------
get_stock_str(stock_list) + get_attribute_str(attribute_list) --- 将list转为字符串，供SQL使用
exchange_sequence(target_list) --- 二维list的交换顺序
cal_sum(x, direction = -1) --- 计算总和
cal_mean(x, direction = -1) --- 计算均值
corrcoef(x, y, method = 0) --- 计算相关系数
dev_n(x) --- 计算总体标准差
dev_n_1(x) --- 计算样本标准差
find_df_index_list(df_data) --- 找到DataFrame类的Index的Type和List
change_data_format_with_df(origin, target, origin_data, columns_name_list = [], index_name_list = [], use_nan = 0) 
    --- 转换和df相关的数据格式
get_right_format_data_with_df(source_data, raw_result_data_df) --- 得到和来源匹配格式的数据，针对df的格式转换
rounding_off(data, digit_number = 2) --- 数据修约，即保留一定小数位数
change_stock_format(origin, target, origin_list) --- 转换股票代码格式
change_date_format(origin, target, origin_list) --- 转换日期序列格式
trans_daily_to_weekly(daily_date_list) --- 将日频日期转化为周频
get_natural_datelist(beginDate, endDate) --- 获取自然日的序列
get_date_delta(date_str, delta_year, delta_month, direction) --- 找到某一天向前或向后一定时间的一天
construct_date_hirabiki_dict(trading_date_list, natural_date_list) --- 建立date的索引字典
get_all_element_from_dict(the_dict) --- 获取字典中全部key中全部元素的并集
get_dict_difference(dict_A, dict_B, list_order = -1) --- 从一个字典中剔除另一个字典中相同的key中相同的list元素
delete_none(X_list, none_value = None, lower_than_that = 0, exchange = 0, how = "any", thresh = None) --- 进行序列去空操作
element_cal_between_list(A, B, cal_type) --- 进行序列元素间加减运算
sum_to_one(raw_list, simple = 0) --- 将序列进行总和归一化（成为总值为1.0，比例不变的序列）
weighted_mean(A_list, B_list, has_null = 0, use_df = 0) --- 将A序列以B序列为权进行平均
z_score_nomalize(raw_list) --- 将序列进行一般标准化（成为均值0，标准差为1的序列）
BARRA_nomalize(raw_list, weight_list) --- 将序列进行BARRA模式标准化(另外允许None的出现)
shrinkage(raw_list, upper_bound = None, lower_bound = None) --- 将序列中大于3倍标准差的部分拉回到3倍标准差
get_half_life_list(total_length, half_life_value, start_weight = 1.0) --- 计算半衰期序列权重值
descriptors_aaggregate_to_factor(descriptors_list, weight_list) --- 加权求和
MA_point(raw_list, n) --- 计算MA的点，即从最后一个数开始的N个数据的平均
moving_average(raw_list, n) --- 计算MA的序列，MA(n)表示n个数据取平均，如不足n个数的部分，则将已有的数据取平均
NAV_normalize(original_list) --- 将序列标准化为起始项为1.0，后续比例不变的序列
from_ROR_to_NAV(ROR_list) --- 从ROR计算NAV
from_NAV_to_ROR(NAV_list) --- 从NAV计算ROR
cal_R0_from_TR(TR, n) --- 从总收益率计算每期平均收益率
match_data_list(A_date_list, B_date_list, B_value_list) --- 匹配与A序列的时间相对应的B序列的数值序列
divide_interval(time_series_list, proportion) --- 对去除趋势的时间序列进行区段划分
'''

'''
***这两个函数用来将list转为字符串，供SQL使用***
'''
def get_stock_str(stock_list):
    value_str = "("
    i = 0
    while i < len(stock_list):
        value_str += "'"
        value_str += str(stock_list[i])
        value_str += "'"
        value_str += ","
        i += 1
    value_str = value_str[:-1] + ")"
    return value_str

def get_attribute_str(attribute_list):
    attribute_str = ""
    i = 0
    while i < len(attribute_list):
        attribute_str += str(attribute_list[i])
        attribute_str += ", "
        i += 1
    return attribute_str[:-2]

'''
***这个函数用来进行二维list的交换顺序***
***method代表是用map，还是map-zip还是array转置的方式***
***map的方式会将不等长部分写成None放在最前面，map-zip会不输出不等长的，array转置则维持原样***
'''
def exchange_sequence(target_list, method = 0):
    if method == 0:
        return map(list, map(None, *target_list))
    elif method == 1:
        return map(list, zip(*target_list))
    elif method == 2:
        return np.array(target_list).T.tolist()
    else:
        print "Unknown method!"
        return 0

'''
***以下这个函数是用来计算总和的，可以允许空***
***direction为-1为仅一维，direction为0为纵向求值（双list时即为跨小list求值），为1为横向求值（双list时即为小list内求值）***
'''
def cal_sum(x, direction = -1):
    if direction == -1:
        x_series = pd.Series(x)
        if isinstance(x[0], int) == True:
            return int(x_series.sum())
        else:
            return x_series.sum()
    else:
        x_df = pd.DataFrame(x)
        return get_right_format_data_with_df(x, x_df.sum(axis = direction))

'''
***以下这个函数是用来计算均值的，可以允许空***
***direction为-1为仅一维，direction为0为纵向求值（双list时即为跨小list求值），为1为横向求值（双list时即为小list内求值）***
'''
def cal_mean(x, direction = -1):
    if direction == -1:
        x_series = pd.Series(x)
        if isinstance(x[0], int) == True:
            return int(x_series.mean())
        else:
            return x_series.mean()
    else:
        x_df = pd.DataFrame(x)
        return get_right_format_data_with_df(x, x_df.mean(axis = direction))

'''
***以下这个函数是用来计算相关系数的，可以允许空***
***method为0计算的是一般的皮尔逊相关系数，为1计算kendall秩相关系数，为2计算spearman的rank相关系数***
'''
def corrcoef(x, y, method = 0):
    x_series = pd.Series(x)
    y_series = pd.Series(y)
    if method == 0:
        result = x_series.corr(y_series, 'pearson')
    elif method == 1:
        result = x_series.corr(y_series, 'kendall')
    elif method == 2:
        result = x_series.corr(y_series, 'spearman')
    return result

'''
***以下这个函数是用来计算总体标准差的，可以允许空***
'''
def dev_n(x):
    x_series = pd.Series(x)
    return x_series.std(ddof = 0)

'''
***以下这个函数是用来计算样本标准差的，可以允许空***
'''
def dev_n_1(x):
    x_series = pd.Series(x)
    return x_series.std(ddof = 1)

'''
***这个函数是用来找到DataFrame类的Index的Type和List***
***type1为multi-index，type2为基本index，type3为自然序列index***
'''
def find_df_index_list(df_data):
    index_list = []
    df_type = 0
    if type(df_data.index) == pd.core.indexes.multi.MultiIndex:
        length = len(df_data.index.levels)
        i = 0
        while i < length:
            index_list.append(df_data.index.names[i])
            i += 1
        df_type = 1
    elif type(df_data.index) == pd.core.indexes.base.Index:
        index_list.append('index')
        df_type = 2
    elif type(df_data.index) == pd.core.indexes.base.RangeIndex:
        index_list.append('index_range')
        df_type = 3
    else:
        print "We don't know this type of DataFrame.Index!"
    return df_type, index_list

'''
***以下这个函数是用来转换和df相关的数据格式的***
***可转换格式包括list, list of lists, dict of lists, dict, DataFrame, 2-dimension, multi-index***
'''
def change_data_format_with_df(origin, target, origin_data, columns_name_list = [], index_name_list = [], use_nan = 0):
    target_data = []
    if target == "DataFrame":
        if origin == "dict of lists" or origin == "dict":
            columns_length = len(origin_data[origin_data.keys()[0]])
            if columns_length != len(columns_name_list):
                columns_name_list = range(columns_length)
            index_length = len(origin_data.keys()[0])
            if index_length != len(index_name_list):
                index_name_list = range(100, index_length + 100)
            
            if origin == "dict of lists":
                #data_df2 = pd.DataFrame(data_dict).T #it is an OK but slow method
                #data_df3 = pd.DataFrame(data_dict.items()).iloc[:, 1].apply(pd.Series) #it is an too slow method
                target_data = pd.DataFrame.from_items(origin_data.items(), orient = 'index', columns = columns_name_list) #it is a much better one
            elif origin == "dict":
                target_data = pd.DataFrame.from_dict(origin_data, orient = 'index')
                target_data.columns = columns_name_list
                
            if len(index_name_list) <= 1:
                target_data.sort_index(axis = 0, inplace = True)
            else:
                target_data.index = pd.MultiIndex.from_tuples(target_data.index, names = index_name_list)
                target_data.sort_index(axis = 0, level = index_name_list[1], inplace = True, sort_remaining = True)
        elif origin == "list":
            columns_length = 1
            if columns_length != len(columns_name_list):
                columns_name_list = range(columns_length)
            target_data = pd.DataFrame(origin_data, columns = columns_name_list)
        elif origin == "list of lists":
            columns_length = len(origin_data[0])
            if columns_length != len(columns_name_list):
                columns_name_list = range(columns_length)
            target_data = pd.DataFrame(origin_data, columns = columns_name_list)
        else:
            print "We haven't finishing this kind of transformation!"
    elif target in ["list", "list of lists", "dict", "dict of lists"]:
        if origin == "DataFrame":
            if use_nan == 0:
                origin_data = origin_data.where(pd.notnull(origin_data), None)
            if target == "dict of lists":
                target_data = origin_data.T.to_dict(orient = 'list')
            elif target == "list of lists":
                origin_data_np = np.array(origin_data)
                target_data = origin_data_np.tolist()
            elif target == "list":
                if type(origin_data) == pd.core.series.Series:
                    origin_data_sr = origin_data
                else:
                    origin_data_sr = origin_data.iloc[:, 0]
                if origin_data_sr.dtype == np.dtype('int64'):
                    target_data = origin_data_sr.astype(int).tolist()
                else:
                    target_data = origin_data_sr.tolist()
            elif target == "dict":
                if type(origin_data) == pd.core.series.Series:
                    origin_data_sr = origin_data
                else:
                    origin_data_sr = origin_data.iloc[:, 0]
                if origin_data_sr.dtype == np.dtype('int64'):
                    target_data = origin_data_sr.astype(int).to_dict()
                else:
                    target_data = origin_data_sr.to_dict()
        else:
            print "We haven't finishing this kind of transformation!"
    elif target == "2-dimension" and origin == "multi-index":
        target_data = origin_data.unstack()
    elif target == "multi-index" and origin == "2-dimension":
        target_data = origin_data.stack()
    else:
        print "We haven't finishing this kind of transformation!"
    return target_data

'''
***以下这个函数是用来得到和来源匹配格式的数据，针对df的格式转换，这个函数主要内部调用***
'''
def get_right_format_data_with_df(source_data, raw_result_data_df):
    if isinstance(source_data, (list, tuple)) == True:
        if isinstance(source_data[0], (list, tuple)) == True:
            result = change_data_format_with_df("DataFrame", "list of lists", raw_result_data_df)
        else:
            result = change_data_format_with_df("DataFrame", "list", raw_result_data_df)
    else:
        result = raw_result_data_df
    return result

'''
***以下这个函数是用来进行数据修约，即保留一定小数位数***
'''
def rounding_off(data, digit_number = 2):
    data_np = pd.DataFrame(data)
    str_ro = '%.' + str(digit_number) + 'f'
    data_np = data_np.applymap(lambda x: str_ro % x)
    return get_right_format_data_with_df(data, data_np)

'''
***以下这个函数是用来转换股票代码格式的***
***del_unknown表示要不要直接去除无法识别的代码，这只会在加尾巴时生效***
'''
def change_stock_format(origin, target, origin_list, del_unknown = 0):
    target_list = []
    if origin == "with_tail" and target == "no_tail":
        i = 0
        while i < len(origin_list):
            target_list.append(origin_list[i][:6])
            i += 1
    elif origin == "no_tail" and target == "with_tail":
        i = 0
        while i < len(origin_list):
            if origin_list[i][0] == '6':
                target_list.append(origin_list[i] + '.SH')
            elif origin_list[i][0] == '0' or origin_list[i][0] == '3':
                target_list.append(origin_list[i] + '.SZ')
            else:
                if del_unknown == 0:
                    print 'Cannot find a proper form! ' + str(origin_list[i])
                elif del_unknown == 1:
                    pass
                elif del_unknown == 2:
                    target_list.append(np.nan)
            i += 1
    return target_list

'''
***以下这个函数是用来转换日期序列格式的***
'''
def change_date_format(origin, target, origin_data):
    target_list = []
    if origin == "str" and target == "datetime":
        if isinstance(origin_data, (list, tuple, np.ndarray, pd.core.series.Series)) == True:
            data_sr = pd.Series(origin_data)
            target_sr = pd.to_datetime(data_sr, format = '%Y%m%d')
            target_list = target_sr.tolist()
        else:
            print "The function cannot apply on this type pf data now!"
    elif origin == "datetime" and target == "str":
        if isinstance(origin_data, (list, tuple, np.ndarray, pd.core.series.Series)) == True:
            data_sr = pd.Series(origin_data)
            target_sr = data_sr.dt.strftime('%Y%m%d')
            target_list = target_sr.tolist()
        elif isinstance(origin_data, pd.core.frame.DataFrame) == True:
            #以下给Indexes中的dt改正格式
            if type(origin_data.index) == pd.core.indexes.multi.MultiIndex:
                length = len(origin_data.index.levels)
                i = 0
                while i < length:
                    if isinstance(origin_data.index.levels[i][0], pd._libs.tslib.Timestamp) == True:
                        origin_data.index = origin_data.index.set_levels(origin_data.index.levels[i].strftime('%Y%m%d'), level = i)
                    i += 1
            elif type(origin_data.index) == pd.core.indexes.base.Index:
                if isinstance(origin_data.index[0], pd._libs.tslib.Timestamp) == True:
                    origin_data.index = origin_data.index.dt.strftime('%Y%m%d')
            elif type(origin_data.index) == pd.core.indexes.range.RangeIndex:
                pass
            elif type(origin_data.index) == pd.core.indexes.datetimes.DatetimeIndex:
                origin_data.index = origin_data.index.strftime('%Y%m%d')
            else:
                print "We don't know this type of DataFrame.Index!"
            
            #以下给Columns中的dt改正格式
            for column in origin_data.columns:
                if isinstance(origin_data[column][0], pd._libs.tslib.Timestamp) == True:
                    origin_data[column] = origin_data[column].dt.strftime('%Y%m%d')
            
            target_list = origin_data
        else:
            print "The function cannot apply on this type pf data now!"
    return target_list

'''
***以下这个函数是用来取出日频时间中的每周五的部分，如果周五放假则向前取最近的交易日***
'''
def trans_daily_to_weekly(daily_date_list):
    weekly_date_list = []
    last_dt = datetime.strptime(daily_date_list[-1], "%Y%m%d")
    first_dt = datetime.strptime(daily_date_list[0], "%Y%m%d")
    #先寻找距离结束最近的星期五
    if last_dt.weekday() >= 4:
        last_Fri_delta = last_dt.weekday() - 4
    else:
        last_Fri_delta = last_dt.weekday() + 3
    last_Fri_dt = last_dt + dt.timedelta(days = -1 * last_Fri_delta)
    #从后向前循环
    i = len(daily_date_list) - 1
    while i >= 0:
        today_dt = datetime.strptime(daily_date_list[i], "%Y%m%d")
        if today_dt.weekday() == 4:
            weekly_date_list.insert(0, str(today_dt.strftime('%Y%m%d')))
            last_Fri_dt = today_dt + dt.timedelta(days = -7)
        elif today_dt < last_Fri_dt:
            #先寻找距离结束最近的星期五
            if today_dt.weekday() >= 4:
                last_Fri_delta = today_dt.weekday() - 4
            else:
                last_Fri_delta = today_dt.weekday() + 3
            last_Fri_dt = today_dt + dt.timedelta(days = -1 * last_Fri_delta)
            #再将这一天放置入周频列表
            weekly_date_list.insert(0, str(today_dt.strftime('%Y%m%d')))
        else:
            pass
        if last_Fri_dt < first_dt:
            break
        i -= 1
    return weekly_date_list

'''
***以下这个函数是用来将日期划分，划分方式1是按年度划分，2是按最近X年划分***
'''
def date_partition(daily_date_list, part_type):
    start_index_list = []
    if part_type == 1:
        start_index_list.append(0)
        now_year = daily_date_list[0][:4]
        i = 1
        while i < len(daily_date_list):
            this_year = daily_date_list[i][:4]
            if this_year != now_year:
                start_index_list.append(i)
                now_year = this_year
            else:
                pass
            i += 1
    elif part_type == 2:
        today = daily_date_list[-1]
        last_start = str(int(today) - 10000)
        i = len(daily_date_list) - 2
        while i >= 0:
            if daily_date_list[i] < last_start:
                start_index_list.append(i)
                last_start = str(int(last_start) - 10000)
            else:
                pass
            i -= 1
        if 0 not in start_index_list:
            start_index_list.append(0)
    return start_index_list

'''
***以下这个函数是用来获取自然日的序列***
'''
def get_natural_datelist(beginDate, endDate):
    # beginDate, endDate是形如‘20160601’的字符串或datetime格式
    date_l = [datetime.strftime(x, '%Y%m%d') for x in list(pd.date_range(start = beginDate, end = endDate))]
    return date_l

'''
***以下这个函数是用来找到某一天向前或向后一定时间的一天，这一天可能事实上并不存在（如01.31往后一个月返回02.31）***
***direction为1时向后，为-1时向前***
'''
def get_date_delta(date_str, delta_year, delta_month, direction):
    if direction == -1:
        if int(date_str[:6]) % 100 > delta_month:
            new_date = str(int(date_str) - delta_year * 10000 - delta_month * 100)
        else:
            new_date = str(int(date_str) - (delta_year + 1) * 10000 - delta_month * 100 + 1200)
    elif direction == 1:
        if int(date_str[:6]) % 100 + delta_month > 12:
            new_date = str(int(date_str) + (delta_year + 1) * 10000 + delta_month * 100 - 1200)
        else:
            new_date = str(int(date_str) + delta_year * 10000 + delta_month * 100)
    else:
        print "This direction is illegal!"
    return new_date

'''
***建立date的索引字典（第一项里周末为周五，第二项里周末为周一）***
***字典的key为日期序列中的日期，value为处在两个序列中的序号***
'''
def construct_date_hirabiki_dict(trading_date_list, natural_date_list):
    date_index_dict = {}
    j = 0
    i = 0
    while i < len(trading_date_list):
        m = j
        while m < len(natural_date_list):
            if trading_date_list[i] == natural_date_list[m]:
                date_index_dict[trading_date_list[i]] = [i, i]
                j = m
                break
            else:
                date_index_dict[natural_date_list[m]] = [i - 1, i]
            m += 1
        i += 1
        j += 1
    last_date_order = date_index_dict[trading_date_list[-1]][0]
    while j < len(natural_date_list):
        date_index_dict[natural_date_list[j]] = [last_date_order, last_date_order + 1]
        j += 1
    return date_index_dict

'''
***获取字典中全部key中全部元素的并集***
'''
def get_all_element_from_dict(the_dict):
    element_set = set()
    for date in the_dict.keys():
        element_set = element_set | set(the_dict[date])
    return sorted(list(element_set))

'''
***从一个字典中剔除另一个字典中相同的key中相同的list元素***
***list_order为-1表示等待被剔除的dict里每个key对应的list都是一维的，为大于等于零的数则表示是以二维中这一列为匹配项剔除***
'''
def get_dict_difference(dict_A, dict_B, list_order = -1):
    if list_order == -1:
        dict_C = {}
        for date in dict_A.keys():
            if dict_B.has_key(date) == True:
                setA = set(dict_A[date])
                setB = set(dict_B[date])
                onlyInA = setA.difference(setB)
                dict_C[date] = sorted(list(onlyInA))
    else:
        dict_C = {}
        for date in dict_A.keys():
            if dict_B.has_key(date) == True:
                dict_C[date] = []
                for cell_list in dict_A[date]:
                    if cell_list[list_order] in dict_B[date]:
                        pass
                    else:
                        dict_C[date].append(cell_list)
    return dict_C

'''
***进行序列去空操作，X_list可为单重或双重list，或者df，none_value表示“空”的定义，如果启用lower_than_that，则是小于none_value的都赋成none（必须为float）***
***X中小List存数据维度，大List存数据条数，如相反，需让exchange = 1***
***如果有thresh，则表示若含有这么多非空，则保留；如没有，则使用how，any表示有None就去除，all表示全是None才去除***
'''
def delete_none(X_list, none_value = None, lower_than_that = 0, exchange = 0, how = "any", thresh = None):
    if exchange == 1:
        X_df = pd.DataFrame(X_list).T
    else:
        X_df = pd.DataFrame(X_list)
    
    if lower_than_that == 1:
        num = X_df._get_numeric_data()
        num[num <= none_value] = np.nan
    else:
        if none_value != None:
            X_df.replace(to_replace = none_value, value = np.nan, inplace = True)
        else:
            pass
    
    if thresh == None:
        X_df.dropna(how = how, inplace = True)
    else:
        X_df.dropna(thresh = thresh, inplace = True)
        
    #print X_df
    if exchange == 1:
        result = get_right_format_data_with_df(X_list, X_df.T)
    else:
        result = get_right_format_data_with_df(X_list, X_df)
    return result

'''
***进行序列间元素对运算（B可以是单个数字，即A中每个都对B计算）***
'''
def element_cal_between_list(A, B, cal_type):
    if isinstance(B, (float, int)) == True:
        a_series = pd.Series(A)
        if cal_type == "+":
            result = a_series + B
        elif cal_type == "-":
            result = a_series - B
        elif cal_type == "*":
            result = a_series * B
        elif cal_type == "/":
            result = a_series / B
        else:
            print "Unknown cal type!"
            return 0
    else:
        a_series = pd.Series(A)
        b_series = pd.Series(B)
        if cal_type == "+":
            result = a_series + b_series
        elif cal_type == "-":
            result = a_series - b_series
        elif cal_type == "*":
            result = a_series * b_series
        elif cal_type == "/":
            result = a_series / b_series
        else:
            print "Unknown cal type!"
            return 0
    return get_right_format_data_with_df(A, result)

'''
***将序列进行总和归一化（成为总值为1.0，比例不变的序列），空值不纳入计算***
***simple是为了加快计算速度，但不可存在空值***
'''
def sum_to_one(raw_list, simple = 0):
    if simple == 0:
        list_sum = cal_sum(raw_list)
        return element_cal_between_list(raw_list, list_sum, "/")
    else:
        sum_value = sum(raw_list)
        result_list = []
        for value in raw_list:
            result_list.append(value / sum_value)
        return result_list

'''
***将A序列以B序列为权进行平均，has_null指是不是存在以-1.0或None/np.nan代替的空变量，1为用-1，2为用None***
***如果要使用df，则数据规模需要较大才划算***
'''
def weighted_mean(A_list, B_list, has_null = 0, use_df = 0):
    if len(A_list) != len(B_list):
        print "Error! Data and weight have different amount!"
        return 0
    elif use_df == 0:
        if has_null == 1:
            repaired_A_list = []
            repaired_B_list = []
            i = 0
            while i < len(A_list):
                if A_list[i] < -0.0001:
                    pass
                else:
                    repaired_A_list.append(A_list[i])
                    repaired_B_list.append(B_list[i])
                i += 1
            if len(repaired_A_list) == 0:
                return None
            else:
                standard_B_list = sum_to_one(repaired_B_list, 1)
                A_np = np.array(repaired_A_list)
                B_np = np.array(standard_B_list)
                return np.dot(A_np, B_np)
        elif has_null == 2:
            repaired_A_list = []
            repaired_B_list = []
            i = 0
            while i < len(A_list):
                if A_list[i] == None or B_list[i] == None:
                    pass
                elif math.isnan(A_list[i]) == True or math.isnan(B_list[i]) == True:
                    pass
                elif A_list[i] == np.nan or B_list[i] == np.nan:
                    pass
                else:
                    repaired_A_list.append(A_list[i])
                    repaired_B_list.append(B_list[i])
                i += 1
            if len(repaired_A_list) == 0:
                return None
            else:
                standard_B_list = sum_to_one(repaired_B_list, 1)
                sum_value = 0.0
                i = 0
                while i < len(repaired_A_list):
                    sum_value += repaired_A_list[i] * standard_B_list[i]
                    i += 1
                return sum_value
        else:
            standard_B_list = sum_to_one(B_list, 1)
            A_np = np.array(A_list)
            B_np = np.array(standard_B_list)
            return np.dot(A_np, B_np)
    else:
        A_df = pd.DataFrame(A_list)
        B_df = pd.DataFrame(B_list)
        if has_null == 1:
            A_df[A_df < 0] = np.nan
            C_df = pd.concat([A_df, B_df], axis = 1)
            C_df = delete_none(C_df)
            D_df = C_df.iloc[:, 1] / C_df.iloc[:, 1].sum() * C_df.iloc[:, 0]
            return D_df.sum()
        elif has_null == 2:
            C_df = pd.concat([A_df, B_df], axis = 1)
            C_df = delete_none(C_df)
            D_df = C_df.iloc[:, 1] / C_df.iloc[:, 1].sum() * C_df.iloc[:, 0]
            return D_df.sum()
        else:
            C_df = pd.concat([A_df, B_df], axis = 1)
            D_df = C_df.iloc[:, 1] / C_df.iloc[:, 1].sum() * C_df.iloc[:, 0]
            return D_df.sum()
    
'''
***将序列进行一般标准化（成为均值0，标准差为1的序列）***
'''
def z_score_nomalize(raw_list):
    raw_df = pd.DataFrame(raw_list)
    raw_std = raw_df.std(ddof = 1)
    raw_mean = raw_df.mean()
    result_df = (raw_df - raw_mean) / raw_std
    return get_right_format_data_with_df(raw_list, result_df)
    
'''
***将序列进行BARRA模式标准化（以流通市值作为均值权重，等权标准差，此外允许None的出现）***
'''
def BARRA_nomalize(raw_list, weight_list):
    raw_df = pd.DataFrame(raw_list)
    raw_std = raw_df.std(ddof = 1)
    raw_mean = raw_df.apply(lambda x: weighted_mean(x, weight_list, 2), axis = 0)
    result_df = (raw_df - raw_mean) / raw_std
    return get_right_format_data_with_df(raw_list, result_df)

'''
***将序列中超出范围的值固定为范围最大/最小值，默认为将大于3倍标准差的部分拉回到3倍标准差***
'''
def shrinkage(raw_list, lower_bound = None, upper_bound = None):
    raw_df = pd.DataFrame(raw_list)
    if upper_bound == None and lower_bound == None:
        raw_std_sr = raw_df.std(ddof = 1)
        raw_mean_sr = raw_df.mean(axis = 0)
        result_df = raw_df.clip(raw_mean_sr - 3.0 * raw_std_sr, raw_mean_sr + 3.0 * raw_std_sr, axis = 1)
    else:
        result_df = raw_df.clip(lower_bound, upper_bound, axis = 1)
    #print raw_mean_sr - 3.0 * raw_std_sr, raw_mean_sr + 3.0 * raw_std_sr
    result = get_right_format_data_with_df(raw_list, result_df)
    return result

'''
***根据给定的半衰期长度、半衰期，计算半衰期序列权重值***
'''
def get_half_life_list(total_length, half_life_value, start_weight = 1.0):
    half_life_list = []
    i = 0
    while i < int(total_length):
        half_life_temp_weight = start_weight * math.pow(0.5, float(i) / float(half_life_value))
        half_life_list.append(half_life_temp_weight)
        i += 1
    return half_life_list

'''
***加权求和***
***可用作将BARRA类因子描述量聚合成因子（若存在None描述量，则放大其他的权重，均为None则最终因子为None）***
***描述量list最外层是不同stock，里面是同factor的descriptors***
'''
def descriptors_aggregate_to_factor(descriptors_list, weight_list):
    descriptors_df = pd.DataFrame(descriptors_list)
    factor_sr = descriptors_df.apply(lambda x: weighted_mean(x, weight_list, 2), axis = 1)
    if isinstance(descriptors_list, (list, tuple)) == True:
        result = change_data_format_with_df("DataFrame", "list", factor_sr)
    else:
        result = factor_sr
    return result

'''
***计算MA的点，即从最后一个数开始的N个数据的平均***
'''
def MA_point(raw_list, n):
    temp_sum = sum(raw_list[-n:])
    return temp_sum / float(n)

'''
***计算MA的序列，MA(n)表示n个数据取平均，如不足n个数的部分，则将已有的数据取平均***
'''
def moving_average(raw_list, n):
    result_list = []
    i = 0
    temp_sum = 0.0
    while i < len(raw_list):
        if i < n - 1:
            temp_sum += raw_list[i]
            result_list.append(temp_sum / (i + 1))
        else:
            temp_sum += raw_list[i]
            temp_sum -= raw_list[i - n]
            result_list.append(temp_sum / float(n))
        i += 1
    return result_list    

'''
***将序列标准化为起始项为1.0，后续比例不变的序列***
'''
def NAV_normalize(original_list):
    if isinstance(original_list, pd.core.frame.DataFrame) == True:
        return original_list / original_list.iloc[0, :]
    else:
        begin_value = float(original_list[0])
        return element_cal_between_list(original_list, begin_value, "/")

'''
***从ROR计算NAV***
'''
def from_ROR_to_NAV(ROR_list):
    NAV_list = [1.0]
    for ROR in ROR_list:
        NAV_last = NAV_list[-1]
        NAV_list.append(NAV_last * (1.0 + ROR))
    return NAV_list

'''
***从NAV计算ROR***
'''
def from_NAV_to_ROR(NAV_list):
    ROR_list = []
    i = 1
    while i < len(NAV_list):
        ROR = NAV_list[i] / NAV_list[i - 1] - 1.0
        ROR_list.append(ROR)
        i += 1
    return ROR_list

'''
***从总收益率计算每期平均收益率***
'''
def cal_R0_from_TR(TR, n):
    return (TR + 1.0) ** (1.0 / n) - 1.0

'''
***匹配与A序列的时间相对应的B序列的数值序列***
***B_value_list可为一维或多维，可为df（但那还用这个函数干嘛？）***
'''
def match_data_list(A_date_list, B_date_list, B_value_list):
    B_df = pd.DataFrame(B_value_list, index = B_date_list)
    A_value_df = B_df.loc[A_date_list]
    result = get_right_format_data_with_df(B_value_list, A_value_df)
    return result

'''
***对去除趋势的时间序列进行区段划分，其中proportion表示在达到max-min的这个比例以上波动才会被识别并划分***
'''
def divide_interval(time_series_list, proportion):
    whole_length = max(time_series_list) - min(time_series_list)
    max_id_list = []
    min_id_list = []
    i = 0
    while i < len(time_series_list) - 1:
        future_max_id = 0
        future_min_id = 0
        first_direction = 0
        '''第一遍循环找出下一个是极大值还是极小值，以及未来区段的范围'''
        j = i + 1
        while j < len(time_series_list):
            if future_max_id == 0 and time_series_list[j] - min(time_series_list[i:j]) > proportion * whole_length:
                future_max_id = j
                if first_direction == 0:
                    first_direction = 1
            elif future_min_id == 0 and max(time_series_list[i:j]) - time_series_list[j] > proportion * whole_length:
                future_min_id = j
                if first_direction == 0:
                    first_direction = -1
            j += 1
        '''第二遍循环找出符合要求的极值序号'''
        if first_direction == 0:
            break
        elif future_max_id == 0:
            future_max_id = len(time_series_list) - 1
        elif future_min_id == 0:
            future_min_id = len(time_series_list) - 1
        if first_direction == 1:
            max_value_temp = max(time_series_list[i + 1: future_min_id])
            max_id_temp = time_series_list[i + 1: future_min_id].index(max_value_temp) + i + 1
            max_id_list.append(max_id_temp)
            i = max_id_temp
        else:
            min_value_temp = min(time_series_list[i + 1: future_max_id])
            min_id_temp = time_series_list[i + 1: future_max_id].index(min_value_temp) + i + 1
            min_id_list.append(min_id_temp)
            i = min_id_temp
    return max_id_list, min_id_list
        