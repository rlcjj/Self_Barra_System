# -*- coding: utf-8 -*-
"""
Created on Thu Mar 23 09:17:22 2017

@author: user
"""

import os
import MySQLdb
from datetime import datetime
import datetime as dt
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
import numpy as np
import pandas as pd
import xyk_common_wind_db_interaction

'''
------目录------
insert_df_replace(table_name, data_df, index_name_list = [])
    --- 针对DataFrame类数据，以创建型方式向某数据表中插入，即每次创建新的表并插入
insert_df_afresh(table_name, data_df, index_name_list = [])
    --- 针对DataFrame类数据，以擦除型方式向某数据表中插入，即每次先清空再重新插入
insert_df_append(table_name, data_df, index_name_list = [])
    --- 针对DataFrame类数据，以添加型方式向某数据表中插入，即每次直接插入（但key相同会报错）
insert_df_commonly(table_name, data_df, index_name_list = [], attribute_name_list = [])
    --- 针对DataFrame类数据，以通用型方式向某数据表中插入，即有重复的，则更新columns里对应的数据
insert_attributes_afresh(table_name, value_list, attribute_list)
    --- 以擦除型方式向某数据表中插入一些数据，即每次先清空再重新插入
insert_attributes_commonly(table_name, value_list, attribute_list, update_list, batch = 100000)
    --- 以通用型方式向某数据表中插入一些数据
get_data_list(table_name, data_attri_list, keep_none = 0, keep_datetime = 0, keep_unicode = 0)
    --- list型获取数据的方式，指定一定维度的数据，返回一个包含这些数据的list，每个小list为对应一条数据
get_data_commonly(table_name, data_attri_list, key_attri_list, keep_none = 0, keep_datetime = 0, keep_unicode = 0, where = "")
    --- 通用型获取数据的方式，指定一定维度的数据，一定维度的key，则返回一个对应key的dict，每小项为对应数据
get_data_df(table_name, data_attri_list, key_attri_list, dt_to_str = 1)
    --- 使用dataframe型获取数据的方式，指定一定维度的数据，一定维度的key，则返回一个对应multi-index的df
get_daily_stock_list(date, forbid_list)
    --- 从state中找出符合要求的股票代码列表
get_daily_data_dict(start_date, end_date, table_name, attribute_list, stock_list = [], is_str = 0, to_df = 0)
    --- 从各股数据表中找出特定股票的一定长度的特定数据，以股票代码和日期两维数据作key
get_daily_data_dict_1_key(start_date, end_date, table_name, attribute_list, stock_list = [], is_str = 0, date_for_key = 0, to_df = 0)
    --- 从各股数据表中找出特定股票的一定长度的特定数据，以一维作key，另一维则放在key所指向的内容第一项
get_daily_data_df(start_date, end_date, table_name, attribute_list, stock_list = [], dt_to_str = 1, date_first = 0)
    --- 这个函数是以DataFrame格式取出时间序列数据，以股票代码和日期两维数据作multi-index
'''

'''
***以下为基本函数与类***
'''
class database():
    def __init__(self,txtfile):
        self.txtfile=os.path.join('c:\\mouhaima\\data_sql',txtfile)

        self.host=file(self.txtfile,'r').read().split('\n')[0].strip()
        self.user=file(self.txtfile,'r').read().split('\n')[1].strip()
        self.passwd=file(self.txtfile,'r').read().split('\n')[2].strip()
        self.db=file(self.txtfile,'r').read().split('\n')[3].strip()

        self.conn=MySQLdb.connect(host=self.host,user=self.user,passwd=self.passwd,db=self.db,charset='utf8')
        self.cursor=self.conn.cursor()
        self.cursor.execute("set interactive_timeout=24*3600")
        self.rows=None
        
    def select(self,str):
        self.cursor.execute(str)
        self.rows=self.cursor.fetchall()
        return self.rows

    def isindexed(self,str):
        self.cursor.execute(str)
        u=self.cursor.fetchone()
        if u!=None:
            return True
        return False

    def insertInf(self,str):
        self.cursor.execute(str)
        self.conn.commit()

    def execute(self,str1):
        self.cursor.execute(str1)
        self.conn.commit()
        
def df_get_engine(txtfile):
    txtfile = os.path.join('c:\\mouhaima\\data_sql',txtfile)
    host=file(txtfile,'r').read().split('\n')[0].strip()
    user=file(txtfile,'r').read().split('\n')[1].strip()
    passwd=file(txtfile,'r').read().split('\n')[2].strip()
    db=file(txtfile,'r').read().split('\n')[3].strip()
    engine_str = "mysql+mysqldb://" + user + ":" + passwd + "@" + host + ":3306/" + db
    return engine_str
        
def get_attribute_str(attribute_list):
    attribute_str = ""
    i = 0
    while i < len(attribute_list):
        attribute_str += str(attribute_list[i])
        attribute_str += ", "
        i += 1
    return attribute_str[:-2]

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

def get_insert_str(value_vec):
    value_str = "("
    i = 0
    while i < len(value_vec):
        if isinstance(value_vec[i],(int,float)) == True:
            value_str += str(value_vec[i])
            value_str += ","
        elif value_vec[i] == "NULL":
            value_str += str(value_vec[i])
            value_str += ","
        else:
            value_str += "'"
            value_str += str(value_vec[i])
            value_str += "'"
            value_str += ","
        i += 1
    value_str = value_str[:-1] + ")"
    return value_str

def get_insert_str_mat(value_list):
    whole_value_str = ""
    i = 0
    while i < len(value_list):
        value_str = get_insert_str(value_list[i])
        whole_value_str += value_str
        whole_value_str += ","
        i += 1
    whole_value_str = whole_value_str[:-1]
    return whole_value_str

def get_update_str(update_list):
    update_str = ""
    i = 0
    while i < len(update_list):
        update_str += update_list[i]
        update_str += " = VALUES(" + update_list[i] + "), "
        i += 1
    update_str = update_str[:-2]
    return update_str

'''这个函数是用来找到DataFrame类的Index的Type和List'''
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
    elif type(df_data.index) == pd.core.indexes.datetimes.DatetimeIndex:
        index_list.append('index_dt')
        df_type = 4
    else:
        print "We don't know this type of DataFrame.Index!"
    return df_type, index_list

'''这个函数是用来找到DataFrame类的Indexes中的text index'''
def find_df_text_index(df_data):
    text_index_list = []
    if type(df_data.index) == pd.core.indexes.multi.MultiIndex:
        length = len(df_data.index.levels)
        i = 0
        while i < length:
            name = df_data.index.names[i]
            if isinstance(df_data.index.levels[i][0],(str,unicode)) == True:
                text_index_list.append(name)
            i += 1
    elif type(df_data.index) == pd.core.indexes.base.Index:
        if isinstance(df_data.index[0],(str,unicode)) == True:
            text_index_list.append('index')
    elif type(df_data.index) == pd.core.indexes.range.RangeIndex:
        pass
    elif type(df_data.index) == pd.core.indexes.datetimes.DatetimeIndex:
        pass
    else:
        print "We don't know this type of DataFrame.Index!"
    return text_index_list

'''这个函数是用来转换df日期序列格式的'''
def change_df_date_format(origin_data):
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
        pass
    
    #以下给Columns中的dt改正格式
    for column in origin_data.columns:
        if isinstance(origin_data[column][0], pd._libs.tslib.Timestamp) == True:
            origin_data[column] = origin_data[column].dt.strftime('%Y%m%d')
    
    return origin_data

'''
***以下为数据库插入操作***
'''

'''这个函数是针对DataFrame类数据，以创建型方式向某数据表中插入，即每次创建新的表并插入'''
def insert_df_replace(table_name, data_df, index_name_list = [], index_dt_level = -1):
    engine_str = df_get_engine('sql_myhost.txt')
    engine = create_engine(engine_str)
    df_type, index_list = find_df_index_list(data_df)
    if df_type == 1:
        if index_dt_level != -1:
            data_df.index = data_df.index.set_levels(pd.to_datetime(pd.Series(data_df.index.levels[0]), format = '%Y%m%d').dt.date, level = index_dt_level)
    else:
        if index_dt_level != -1:
            data_df = data_df.set_index(pd.to_datetime(data_df.index.values, format = '%Y%m%d').date)
    if len(index_list) == len(index_name_list):
        i = 0
        while i < len(index_list):
            index_list[i] = index_name_list[i]
            i += 1
    text_index_list = find_df_text_index(data_df)
    if len(text_index_list) > 2:
        print "We haven't known how to deal with DataFrame with more than 2 text keys!"
    elif len(text_index_list) == 2:
        #print 1
        data_df.to_sql(table_name, engine, if_exists = 'replace', index_label = index_list, chunksize = 500, dtype = {text_index_list[0]:VARCHAR(data_df.index.get_level_values(text_index_list[0]).str.len().max()), text_index_list[1]:VARCHAR(data_df.index.get_level_values(text_index_list[1]).str.len().max())})
        #print 2
    elif len(text_index_list) == 1:
        data_df.to_sql(table_name, engine, if_exists = 'replace', index_label = index_list, chunksize = 500, dtype = {text_index_list[0]:VARCHAR(data_df.index.get_level_values(text_index_list[0]).str.len().max())})
    else:
        data_df.to_sql(table_name, engine, if_exists = 'replace', index_label = index_list, chunksize = 500)
    engine.dispose()
    return 0

'''这个函数是针对DataFrame类数据，以擦除型方式向某数据表中插入，即每次先清空再重新插入'''
def insert_df_afresh(table_name, data_df, index_name_list = []):
    thbase = database('sql_myhost.txt')
    str1 = "DELETE FROM  " + table_name
    thbase.execute(str1)
    engine_str = df_get_engine('sql_myhost.txt')
    engine = create_engine(engine_str)
    df_type, index_list = find_df_index_list(data_df)
    if len(index_list) == len(index_name_list):
        i = 0
        while i < len(index_list):
            index_list[i] = index_name_list[i]
            i += 1
    text_index_list = find_df_text_index(data_df)
    if len(text_index_list) > 2:
        print "We haven't known how to deal with DataFrame with more than 2 text keys!"
    elif len(text_index_list) == 2:
        data_df.to_sql(table_name, engine, if_exists = 'append', index_label = index_list, chunksize = 500, dtype = {text_index_list[0]:VARCHAR(data_df.index.get_level_values(text_index_list[0]).str.len().max()), text_index_list[1]:VARCHAR(data_df.index.get_level_values(text_index_list[1]).str.len().max())})
    elif len(text_index_list) == 1:
        data_df.to_sql(table_name, engine, if_exists='append', index_label = index_list, chunksize = 500, dtype = {text_index_list[0]:VARCHAR(data_df.index.get_level_values(text_index_list[0]).str.len().max())})
    else:
        data_df.to_sql(table_name, engine, if_exists = 'append', index_label = index_list, chunksize = 500)
    engine.dispose()
    return 0

'''这个函数是针对DataFrame类数据，以添加型方式向某数据表中插入，即每次直接插入（但key相同会报错）'''
def insert_df_append(table_name, data_df, index_name_list = []):
    engine_str = df_get_engine('sql_myhost.txt')
    engine = create_engine(engine_str)
    df_type, index_list = find_df_index_list(data_df)
    if len(index_list) == len(index_name_list):
        i = 0
        while i < len(index_list):
            index_list[i] = index_name_list[i]
            i += 1
    text_index_list = find_df_text_index(data_df)
    if len(text_index_list) > 2:
        print "We haven't known how to deal with DataFrame with more than 2 text keys!"
    elif len(text_index_list) == 2:
        data_df.to_sql(table_name, engine, if_exists = 'append', index_label = index_list, chunksize = 500, dtype = {text_index_list[0]:VARCHAR(data_df.index.get_level_values(text_index_list[0]).str.len().max()), text_index_list[1]:VARCHAR(data_df.index.get_level_values(text_index_list[1]).str.len().max())})
    elif len(text_index_list) == 1:
        data_df.to_sql(table_name, engine, if_exists='append', index_label = index_list, chunksize = 500, dtype = {text_index_list[0]:VARCHAR(data_df.index.get_level_values(text_index_list[0]).str.len().max())})
    else:
        data_df.to_sql(table_name, engine, if_exists = 'append', index_label = index_list, chunksize = 500)
    engine.dispose()
    return 0

'''这个函数是针对DataFrame类数据，以通用型方式向某数据表中插入，即有重复的，则更新columns里对应的数据'''
def insert_df_commonly(table_name, data_df, index_name_list = [], attribute_name_list = [], index_dt_level = -1):
    insert_df_replace("df_temp_table", data_df, index_name_list, index_dt_level = index_dt_level)
    df_type, index_list = find_df_index_list(data_df)
    if len(index_list) == len(index_name_list):
        i = 0
        while i < len(index_list):
            index_list[i] = index_name_list[i]
            i += 1
    columns_list = []
    i = 0
    while i < len(data_df.columns):
        columns_list.append(data_df.columns[i])
        i += 1
    if len(columns_list) == len(attribute_name_list):
        i = 0
        while i < len(columns_list):
            columns_list[i] = attribute_name_list[i]
            i += 1
    set_str = ""
    i = 0
    while i < len(columns_list):
        if i == len(columns_list) - 1:
            set_str += "a." + columns_list[i] + " = b." + columns_list[i]
        else:
            set_str += "a." + columns_list[i] + " = b." + columns_list[i] + ", "
        i += 1
    where_str = ""
    i = 0
    while i < len(index_list):
        if i == len(index_list) - 1:
            where_str += "a." + index_list[i] + " = b." + index_list[i]
        else:
            where_str += "a." + index_list[i] + " = b." + index_list[i] + " AND "
        i += 1
    str1 = "UPDATE " + table_name + " as a, df_temp_table as b \
            SET " + set_str + " \
            WHERE " + where_str
    #print str1
    thbase = database('sql_myhost.txt')
    thbase.execute(str1)
    return 0

'''这个函数是以擦除型方式向某数据表中插入一些数据，即每次先清空再重新插入'''
def insert_attributes_afresh(table_name, value_list, attribute_list):
    thbase = database('sql_myhost.txt')
    str1 = "DELETE FROM  " + table_name
    attribute_str = get_attribute_str(attribute_list)
    str2 = "INSERT INTO " + table_name + " (" + attribute_str + ")\
            VALUES "
    str2 += get_insert_str_mat(value_list)
    #print str1
    thbase.execute(str1)
    thbase.execute(str2) 
    return 0

'''这个函数是以通用型方式向某数据表中插入一些数据'''
'''如果有重复的，则更新update_list里面的attributes'''
def insert_attributes_commonly(table_name, value_list, attribute_list, update_list, batch = 100000):
    thbase = database('sql_myhost.txt')
    attribute_str = get_attribute_str(attribute_list)
    update_str = get_update_str(update_list)
    i = 0
    while i < (len(value_list) / batch):
        print "Now is the " + str(i + 1) + "th " + str(batch) + " data..."
        str1 = "INSERT INTO " + table_name + " (" + attribute_str + ")\
                VALUES "
        str1 += get_insert_str_mat(value_list[batch * i: batch * (i + 1)])
        str1 += " ON DUPLICATE KEY UPDATE " +  update_str
        #print str1
        thbase.execute(str1)    
        i += 1
    if batch * i != len(value_list):
        if i != 0:
            print "Now is the " + str(i + 1) + "th " + str(batch) + " data..."
        str1 = "INSERT INTO " + table_name + " (" + attribute_str + ")\
                VALUES "
        str1 += get_insert_str_mat(value_list[batch * i:])
        str1 += " ON DUPLICATE KEY UPDATE " +  update_str
        #print str1
        thbase.execute(str1)  
    return 0

'''以下为已经停止使用的部分函数'''
'''------------------------------------------------------------'''

'''这个函数是向每日（或每月）个股数据集中插入一些数据'''
def insert_daily_stock_attribute(table_name, value_list, attribute_name):
    thbase = database('sql_myhost.txt')
    str1 = "INSERT INTO " + table_name + " (stock_id, curr_date, " +  attribute_name + ")\
            VALUES "
    str1 += get_insert_str_mat(value_list)
    str1 += " ON DUPLICATE KEY UPDATE " +  attribute_name + " = VALUES(" +  attribute_name + ")"
    #print str1
    thbase.execute(str1)    
    return 0

'''这个函数是向个股基本信息集中插入一些数据'''
def insert_stock_basic_info(value_list):
    thbase = database('sql_myhost.txt')
    str1 = "INSERT INTO stock_basic_info (stock_id, list_board, list_date, delist_date, pinyin)\
            VALUES "
    str1 += get_insert_str_mat(value_list)
    str1 += " ON DUPLICATE KEY UPDATE list_board = VALUES(list_board), list_date = VALUES(list_date), \
            delist_date = VALUES(delist_date), pinyin = VALUES(pinyin)"
    thbase.execute(str1)    
    return 0

'''这个函数是向个股所属概念板块信息集中插入一些数据'''
def insert_monthly_stock_conception(value_list):
    thbase = database('sql_myhost.txt')
    str1 = "INSERT INTO monthly_stock_conception (stock_id, curr_date, wind_sec_code, is_in)\
            VALUES "
    str1 += get_insert_str_mat(value_list)
    str1 += " ON DUPLICATE KEY UPDATE is_in = VALUES(is_in)"
    thbase.execute(str1)    
    return 0

'''这个函数是向每日指数权重集中插入一些数据'''
def insert_daily_style_weight(value_list):
    thbase = database('sql_myhost.txt')
    str1 = "INSERT INTO daily_style_weight (style_id, curr_date, stock_id, weight)\
            VALUES "
    str1 += get_insert_str_mat(value_list)
    str1 += " ON DUPLICATE KEY UPDATE weight = VALUES(weight)"
    thbase.execute(str1)    
    return 0

'''这个函数是向每日指数点数集中插入一些数据'''
def insert_daily_style_point(value_list):
    thbase = database('sql_myhost.txt')
    str1 = "INSERT INTO daily_style_point (style_id, curr_date, point)\
            VALUES "
    str1 += get_insert_str_mat(value_list)
    str1 += " ON DUPLICATE KEY UPDATE point = VALUES(point)"
    thbase.execute(str1)    
    return 0

'''这个函数是向每日指数信号集中插入一些数据'''
def insert_daily_style_signals(value_list):
    thbase = database('sql_myhost.txt')
    str1 = "INSERT INTO daily_style_signals (style_id, curr_date, signal_id, signal_value)\
            VALUES "
    str1 += get_insert_str_mat(value_list)
    str1 += " ON DUPLICATE KEY UPDATE signal_value = VALUES(signal_value)"
    thbase.execute(str1)    
    return 0

'''这个函数是向每日指数数据差数据集中插入一些数据'''
def insert_daily_style_pair_spread(value_list):
    thbase = database('sql_myhost.txt')
    str1 = "INSERT INTO daily_style_pair_spread (style_pair_id, curr_date, spread_type, spread_value)\
            VALUES "
    str1 += get_insert_str_mat(value_list)
    str1 += " ON DUPLICATE KEY UPDATE spread_value = VALUES(spread_value)"
    thbase.execute(str1)    
    return 0

'''这个函数是向信号+指数数据集中插入一些数据'''
def insert_signal_index_data(value_list):
    thbase = database('sql_myhost.txt')
    str1 = "INSERT INTO signal_index_data (curr_date, style_pair_id, signal_name, index_value, signal_value)\
            VALUES "
    str1 += get_insert_str_mat(value_list)
    str1 += " ON DUPLICATE KEY UPDATE index_value = VALUES(index_value), signal_value = VALUES(signal_value)"
    thbase.execute(str1)    
    return 0

'''------------------------------------------------------------'''

'''
***以下为数据库更新操作***
'''

'''这个函数是将EPS数据中空的地方填补成以前的数据使用的'''
def daily_EPS_data_patching():
    thbase = database('sql_myhost.txt')
    str1="SELECT stock_id, curr_date, EPS FROM daily_stock_profit \
         WHERE curr_date < '%s' AND ROE IS not null ORDER BY stock_id, curr_date asc" %("20061231")
    rows1 = thbase.select(str1)
    EPS_data_list = []
    last_stock = ""
    for line in rows1:
        if line[2] != None:
            #print line[2]
            last_EPS = float(line[2])
            last_stock = str(line[0])
            #EPS_data_list.append([str(line[0]), str(line[1]), float(line[2])])
        elif str(line[0]) == last_stock:
            EPS_data_list.append([str(line[0]), str(line[1]), last_EPS])
        else:
            pass
    i = 0
    while i < (len(EPS_data_list) / 100000):
        print "Now is the " + str(i) + "th 100000 data..."
        insert_daily_stock_attribute("daily_stock_profit", EPS_data_list[(100000 * i): (100000 * (i + 1))], "EPS")
        i += 1
    insert_daily_stock_attribute("daily_stock_profit", EPS_data_list[(100000 * i):], "EPS")
    return 0

'''这个函数是在信号+指数数据集中添加SMS信号'''
def SMS_signal_adding():
    style_pair_amount = 8
    i = 0
    while i < style_pair_amount:
        date_list, index_value_list, signal_value_list = get_signal_index_data(str(i), "MID")
        value_list = []
        j = 0
        while j < len(date_list):
            value_list.append([date_list[j], str(i), "SMS", index_value_list[j], index_value_list[j]])
            j += 1
        insert_signal_index_data(value_list)
        i += 1
    return 0

'''
***以下为数据库SELECT操作***
'''

'''这个函数是list型获取数据的方式，指定一定维度的数据，返回一个包含这些数据的list，每个小list为对应一条数据'''
'''默认将所有datetime类转为str，将所有unicode转为str，将所有None转为'NULL'。'''
def get_data_list(table_name, data_attri_list, keep_none = 0, keep_datetime = 0, keep_unicode = 0):
    data_attri_str = get_attribute_str(data_attri_list)
    thbase = database('sql_myhost.txt')
    str1 = "select " + data_attri_str + " from " + table_name
    rows1 = thbase.select(str1)
    data_list = []
    for line in rows1:
        data_list_temp = []
        i = 0
        while i < len(line):
            if line[i] is None and keep_none == 0:
                this_data = None
            elif isinstance(line[i], dt.date) == True and keep_datetime == 0:
                this_data = str(line[i].strftime('%Y%m%d'))
            elif isinstance(line[i], unicode) == True and keep_unicode == 0:
                this_data = str(line[i])
            else:
                this_data = line[i]
            if len(data_attri_list) == 1:
                data_list_temp = this_data
            else:
                data_list_temp.append(this_data)
            i += 1
        data_list.append(data_list_temp)
    return data_list

'''这个函数是通用型获取数据的方式，指定一定维度的数据，一定维度的key，则返回一个对应key的dict，每小项为对应数据'''
'''默认将所有datetime类转为str，将所有unicode转为str，将所有None保留'''
def get_data_commonly(table_name, data_attri_list, key_attri_list, keep_none = 0, keep_datetime = 0, keep_unicode = 0, one_to_one = 1, where = ""):
    whole_attribute_list = data_attri_list + key_attri_list
    whole_attribute_str = get_attribute_str(whole_attribute_list)
    thbase=database('sql_myhost.txt')
    if where == "":
        str1="select " + whole_attribute_str + " from " + table_name
    else:
        str1="select " + whole_attribute_str + " from " + table_name + " where " + where
    rows1=thbase.select(str1)
    data_dict = {}
    for line in rows1:
        data_list_temp = []
        key_list_temp = []
        i = 0
        while i < len(line):
            if line[i] is None and keep_none == 0:
                this_data = None
            elif isinstance(line[i], dt.date) == True and keep_datetime == 0:
                this_data = str(line[i].strftime('%Y%m%d'))
            elif isinstance(line[i], unicode) == True and keep_unicode == 0:
                this_data = str(line[i])
            else:
                this_data = line[i]
            if i < len(data_attri_list):
                if len(data_attri_list) == 1:
                    data_list_temp = this_data
                else:
                    data_list_temp.append(this_data)
            else:
                if len(key_attri_list) == 1:
                    key_list_temp = this_data
                else:
                    key_list_temp.append(this_data)
            i += 1
        if len(key_attri_list) == 1:
            if len(data_attri_list) == 1 and one_to_one == 1:
                data_dict[key_list_temp] = data_list_temp
            else:
                if data_dict.has_key(key_list_temp) == True:
                    data_dict[key_list_temp].append(data_list_temp)
                else:
                    data_dict[key_list_temp] = [data_list_temp]
        else:
            if len(data_attri_list) == 1 and one_to_one == 1:
                data_dict[tuple(key_list_temp)] = data_list_temp
            else:
                if data_dict.has_key(tuple(key_list_temp)) == True:
                    data_dict[tuple(key_list_temp)].append(data_list_temp)
                else:
                    data_dict[tuple(key_list_temp)] = [data_list_temp]
    return data_dict

'''这个函数是使用dataframe型获取数据的方式，指定一定维度的数据，一定维度的key，则返回一个对应multi-index的df'''
'''默认将所有datetime类、unicode、None均保留'''
def get_data_df(table_name, data_attri_list, key_attri_list, dt_to_str = 1):
    engine_str = df_get_engine('sql_myhost.txt')
    engine = create_engine(engine_str)
    data_df = pd.read_sql_table(table_name, engine, index_col = key_attri_list, columns = data_attri_list)
    if dt_to_str == 1:
        data_df = change_df_date_format(data_df)
    engine.dispose()
    return data_df

'''这个函数是从state中找出符合要求的股票代码列表'''
def get_daily_stock_list(date, forbid_list):
    thbase=database('sql_myhost.txt')
    
    str1="select stock_id from daily_stock_technical \
        where curr_date = '%s' and close is not null" %(date)
    rows1=thbase.select(str1)
    whole_stock_list = []
    for line in rows1:
        whole_stock_list.append(str(line[0]))
        
    str2="select stock_id, curr_state from daily_stock_state \
        where curr_date = '%s'" %(date)
    rows2=thbase.select(str2)
    forbid_stock_list = []
    for line in rows2:
        if int(line[1]) in forbid_list:
            forbid_stock_list.append(str(line[0]))
            
    for stock in forbid_stock_list:
        if stock in whole_stock_list:
            whole_stock_list.remove(stock)
            
    return whole_stock_list

'''这个函数是从各股数据表中找出特定股票的一定长度的特定数据'''
'''以股票代码和日期两维数据作key'''
'''可以不填写股票代码，则默认取出全部符合条件的股票'''
def get_daily_data_dict(start_date, end_date, table_name, attribute_list, stock_list = [], is_str = 0, to_df = 0):
    attribute_str = get_attribute_str(attribute_list)
    stock_str = get_stock_str(stock_list)
    thbase=database('sql_myhost.txt')
    if len(stock_list) == 0:
        str1="select stock_id, curr_date, " + attribute_str + " from " + table_name + " \
            where curr_date >= '%s' and curr_date <= '%s'" %(start_date, end_date)
    else:
        str1="select stock_id, curr_date, " + attribute_str + " from " + table_name + " \
            where curr_date >= '%s' and curr_date <= '%s' and stock_id in %s" %(start_date, end_date, stock_str)
    rows1=thbase.select(str1)
    data_dict = {}
    if len(attribute_list) == 1:
        for line in rows1:
            if line[2] != None:
                if is_str == 1:
                    value = str(line[2])
                else:
                    value = float(line[2])
            else:
                value = None
            data_dict[str(line[0]), str(line[1].strftime('%Y%m%d'))] = value
    else:
        for line in rows1:
            data_dict[str(line[0]), str(line[1].strftime('%Y%m%d'))] = []
            j = 0
            while j < len(attribute_list):
                if line[j + 2] != None:
                    if is_str == 1:
                        data_dict[str(line[0]), str(line[1].strftime('%Y%m%d'))].append(str(line[j + 2]))
                    else:
                        data_dict[str(line[0]), str(line[1].strftime('%Y%m%d'))].append(float(line[j + 2]))
                else:
                    data_dict[str(line[0]), str(line[1].strftime('%Y%m%d'))].append(None)
                j += 1
    if to_df == 0:
        return data_dict
    else:
        index_name_list = ["stock_id", "curr_date"]
        if len(attribute_list) == 1:
            data_df = pd.DataFrame.from_dict(data_dict, orient = 'index')
            data_df.columns = attribute_list
        else:
            data_df = pd.DataFrame.from_items(data_dict.items(), orient = 'index', columns = attribute_list)
        data_df.index = pd.MultiIndex.from_tuples(data_df.index, names = index_name_list)
        data_df.sort_index(axis = 0, level = index_name_list[1], inplace = True, sort_remaining = True)
        return data_df
        
'''这个函数是从各股数据表中找出特定股票的一定长度的特定数据'''
'''以股票代码一维数据作key，日期放在代码所指向的内容第一项'''
'''也可选择以日期一维数据作key，股票代码放在日期所指向的内容第一项'''
'''可以不填写股票代码，则默认取出全部符合条件的股票'''
def get_daily_data_dict_1_key(start_date, end_date, table_name, attribute_list, stock_list = [], is_str = 0, date_for_key = 0, to_df = 0):
    attribute_str = get_attribute_str(attribute_list)
    stock_str = get_stock_str(stock_list)
    thbase=database('sql_myhost.txt')
    if len(stock_list) == 0:
        str1="select stock_id, curr_date, " + attribute_str + " from " + table_name + " \
            where curr_date >= '%s' and curr_date <= '%s'" %(start_date, end_date)
    else:
        str1="select stock_id, curr_date, " + attribute_str + " from " + table_name + " \
            where curr_date >= '%s' and curr_date <= '%s' and stock_id in %s" %(start_date, end_date, stock_str)
    rows1=thbase.select(str1)
    data_dict = {}
    if date_for_key == 0:
        daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
        for line in rows1:
            #print line
            this_date_str = str(line[1].strftime('%Y%m%d'))
            if this_date_str in daily_date_list:
                if not data_dict.has_key(line[0]):
                    data_dict[line[0]] = []
                daily_data = [this_date_str]
                j = 0
                while j < len(attribute_list):
                    if line[j + 2] != None:
                        if is_str == 1:
                            daily_data.append(str(line[j + 2]))
                        else:
                            daily_data.append(float(line[j + 2]))
                    else:
                        daily_data.append(None)
                    j += 1
                data_dict[line[0]].append(daily_data)
        if to_df != 0:
            for key in data_dict.keys():
                data_dict[key] = pd.DataFrame(data_dict[key], columns = ['curr_date'] + attribute_list).set_index('curr_date')
    else:
        for line in rows1:
            #print line
            if not data_dict.has_key(str(line[1].strftime('%Y%m%d'))):
                data_dict[str(line[1].strftime('%Y%m%d'))] = []
            daily_data = [str(line[0])]
            j = 0
            while j < len(attribute_list):
                if line[j + 2] != None:
                    if is_str == 1:
                        daily_data.append(str(line[j + 2]))
                    else:
                        daily_data.append(float(line[j + 2]))
                else:
                    daily_data.append(None)
                j += 1
            data_dict[str(line[1].strftime('%Y%m%d'))].append(daily_data)
        if to_df != 0:
            for key in data_dict.keys():
                data_dict[key] = pd.DataFrame(data_dict[key], columns = ['stock_id'] + attribute_list).set_index('stock_id')
    return data_dict

'''这个函数是以DataFrame格式取出时间序列数据'''
'''以股票代码和日期两维数据作multi-index'''
'''可以不填写股票代码，则默认取出全部符合条件的股票'''
def get_daily_data_df(start_date, end_date, table_name, attribute_list, stock_list = [], dt_to_str = 1, date_first = 0):
    attribute_str = get_attribute_str(attribute_list)
    stock_str = get_stock_str(stock_list)
    if date_first == 0:
        if len(stock_list) == 0:
            str1="select stock_id, curr_date, " + attribute_str + " from " + table_name + " \
                where curr_date >= '%s' and curr_date <= '%s'" %(start_date, end_date)
        else:
            str1="select stock_id, curr_date, " + attribute_str + " from " + table_name + " \
                where curr_date >= '%s' and curr_date <= '%s' and stock_id in %s" %(start_date, end_date, stock_str)
        engine_str = df_get_engine('sql_myhost.txt')
        engine = create_engine(engine_str)
        data_df = pd.read_sql(str1, engine, index_col = ["stock_id", "curr_date"], columns = attribute_list)
    else:
        if len(stock_list) == 0:
            str1="select curr_date, stock_id, " + attribute_str + " from " + table_name + " \
                where curr_date >= '%s' and curr_date <= '%s'" %(start_date, end_date)
        else:
            str1="select curr_date, stock_id, " + attribute_str + " from " + table_name + " \
                where curr_date >= '%s' and curr_date <= '%s' and stock_id in %s" %(start_date, end_date, stock_str)
        engine_str = df_get_engine('sql_myhost.txt')
        engine = create_engine(engine_str)
        data_df = pd.read_sql(str1, engine, index_col = ["curr_date", "stock_id"], columns = attribute_list)
    if dt_to_str == 1:
        data_df = change_df_date_format(data_df)
    engine.dispose()
    return data_df

'''以下为已经停止使用的部分函数'''
'''------------------------------------------------------------'''

'''这个函数是从基本信息表中获取全部数据'''
def get_stock_basic_data(attribute_list):
    attribute_str = get_attribute_str(attribute_list)
    thbase=database('sql_myhost.txt')
    str1="select stock_id, " + attribute_str + " from stock_basic_info"
    rows1=thbase.select(str1)
    data_dict = {}
    for line in rows1:
        data_dict[str(line[0])] = []
        j = 0
        while j < len(attribute_list):
            if line[j + 1] != None:
                data_dict[str(line[0])].append(str(line[j + 1]))
            else:
                data_dict[str(line[0])].append(None)
            j += 1
    return data_dict

'''这个函数是从指数成分股数据表中找出特定风格的一定长度的成分股及权重'''
def get_daily_style_data(start_date, end_date, style_id):
    thbase=database('sql_myhost.txt')
    str1="select curr_date, stock_id, weight from daily_style_weight \
        where curr_date >= '%s' and curr_date <= '%s' and style_id = '%s' order by curr_date" %(start_date, end_date, style_id)
    rows1=thbase.select(str1)
    data_dict = {}
    whole_stock_list = []
    for line in rows1:
        if data_dict.has_key(str(line[0].strftime('%Y%m%d'))) == False:
            data_dict[str(line[0].strftime('%Y%m%d'))] = [[str(line[1])], [float(line[2])]]
            whole_stock_list.append(str(line[1]))
        else:
            data_dict[str(line[0].strftime('%Y%m%d'))][0].append(str(line[1]))
            data_dict[str(line[0].strftime('%Y%m%d'))][1].append(float(line[2]))
            whole_stock_list.append(str(line[1]))
    whole_stock_list = list(set(whole_stock_list))
    return data_dict, whole_stock_list

'''这个函数是从指数信号数据表中找出特定风格的一定长度的信号值'''
def get_daily_style_signals(start_date, end_date, style_id, signal_id):
    thbase=database('sql_myhost.txt')
    str1="select curr_date, signal_value from daily_style_signals \
        where curr_date >= '%s' and curr_date <= '%s' and style_id = '%s' and signal_id = '%s' order by curr_date" %(start_date, end_date, style_id, signal_id)
    rows1=thbase.select(str1)
    signal_value_list = []
    curr_date_list = []
    for line in rows1:
        curr_date_list.append(str(line[0].strftime('%Y%m%d')))
        signal_value_list.append(float(line[1]))
    return curr_date_list, signal_value_list

'''这个函数是从指数点数数据表中找出特定风格的一定长度的点数值'''
def get_daily_style_point(start_date, end_date, style_id):
    thbase=database('sql_myhost.txt')
    str1="select curr_date, point from daily_style_point \
        where curr_date >= '%s' and curr_date <= '%s' and style_id = '%s' order by curr_date" %(start_date, end_date, style_id)
    rows1=thbase.select(str1)
    signal_point_list = []
    curr_date_list = []
    for line in rows1:
        curr_date_list.append(str(line[0].strftime('%Y%m%d')))
        signal_point_list.append(float(line[1]))
    return curr_date_list, signal_point_list

'''这个函数是从风格配对的估值或一致性等数值差数据表中找出特定风格对的一定长度的数值'''
def get_daily_style_pair_spread(start_date, end_date, style_pair_id, spread_type):
    thbase=database('sql_myhost.txt')
    str1="select curr_date, spread_value from daily_style_pair_spread \
        where curr_date >= '%s' and curr_date <= '%s' and style_pair_id = '%s' and spread_type = '%s' order by curr_date" %(start_date, end_date, style_pair_id, spread_type)
    rows1=thbase.select(str1)
    spread_value_list = []
    curr_date_list = []
    for line in rows1:
        curr_date_list.append(str(line[0].strftime('%Y%m%d')))
        spread_value_list.append(float(line[1]))
    return curr_date_list, spread_value_list

'''这个函数是从信号+指数数据集中找出特定信号与特定风格对的数值'''
def get_signal_index_data(style_pair_id, signal_name):
    thbase=database('sql_myhost.txt')
    str1="select curr_date, index_value, signal_value from signal_index_data \
        where style_pair_id = '%s' and signal_name = '%s' order by curr_date" %(style_pair_id, signal_name)
    rows1=thbase.select(str1)
    date_list = []
    index_value_list = []
    signal_value_list = []
    for line in rows1:
        date_list.append(str(line[0].strftime('%Y%m%d')))
        index_value_list.append(float(line[1]))
        signal_value_list.append(float(line[2]))
    return date_list, index_value_list, signal_value_list

'''------------------------------------------------------------'''

#a_stock_list = get_data_list("a_stock", ["stock_id", "list_date", "delist_date"])
#insert_daily_stock_attribute("daily_stock_validation", [["123456.SZ", "20150101", 1.2], ["132413.SZ", "20150101", 1.5]], "pb")
#whole_stock_list = get_daily_stock_list("20160614", [1,2,3,4])
#data_dict = get_daily_data_dict("20160614", "20170614", "daily_stock_technical", ["close", "open"], ["000001.SZ"])
#style_dict = get_daily_style_data("20160101", "20160131", "0")
#curr_date_list, signal_value_list = get_daily_style_signals("20160101", "20160131", "0", "pb")
#curr_date_list, signal_point_list = get_daily_style_point("20160101", "20160131", "0")
#curr_date_list, validation_spread_list = get_daily_validation_spread("20160101", "20160131", "0", "pb")
#SMS_signal_adding()
