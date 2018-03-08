# -*- coding: utf-8 -*-
"""
Created on Thu Dec 15 20:40:02 2016

@author: MouHaiMa
"""

import xyk_common_data_processing
import os
import MySQLdb

'''
------目录------
get_calendar(start_date, end_date, is_monthly = 1) --- 获取交易日字符串序列
get_all_stock_cube_data(table_name, attribute_list, start_date, end_date) --- 获取全部股票立方体数据
get_all_stock_closing_price(start_date, end_date, minimum_amount) --- 获取全部股票复权后收盘价与昨日收盘价
get_all_stock_ROR(start_date, end_date, minimum_amount) --- 计算出全部股票的ROR字典
get_all_stock_slide_value(table_name, indicator, date) --- 获取全部股票横截面数据
get_all_stock_time_series(table_name, indicator, start_date, end_date, minimum_amount) --- 获取全部股票时间序列数据
get_date_and_index_closing_price(index_code, table, start_date, end_date, minimum_amount) --- 获取日期和指数收盘点数
get_index_weight(last_date, index) --- 计算类沪深300指数的成分股权重
get_all_stock_daily_financial_data(table_name, attribute_list, date) --- 获取全部股票每日财务数据
get_all_stock_balance_sheet_data(date, field) --- 获取所有股票的资产负债表所需字段序列
get_all_stock_Income_ttm_data(date, field) --- 获取所有股票的利润表所需字段序列
get_IBOR_dict(IBOR_type, windcode, count_length = 1) --- 获取全部银行间拆放利率
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
        
DB = database('sql_wind.txt')

'''
***在WIND-DB中查询并获取交易日字符串序列，可选日频或月频***
'''
def get_calendar(start_date, end_date, is_monthly = 1):
    str1 = "select TRADE_DAYS from AShareCalendar " \
                "where TRADE_DAYS >= '%s' and TRADE_DAYS <= '%s' and S_INFO_EXCHMARKET = 'SSE' order by TRADE_DAYS desc" % (start_date, end_date)
    rows = DB.select(str1)
    RDF_calendar_list = []
    if is_monthly == 0: #日频
        for one in rows:
            RDF_calendar_list.append(str(one[0]))
    else: #月频
        for one in rows:
            str_date = str(one[0])
            if len(RDF_calendar_list) == 0:
                RDF_calendar_list.append(str(one[0]))
            elif str_date[:6] != RDF_calendar_list[-1][:6]:
                    RDF_calendar_list.append(str(one[0]))
            else:
                pass
    RDF_calendar_list.reverse()
    return RDF_calendar_list
    
#calendar_list = get_calendar("20160101", "20170101", 0)

'''
***在WIND-DB中查询并获取全部股票立方体数据（字典形式），如果当前处于停盘等异常状态，返回空***
'''
def get_all_stock_cube_data(table_name, attribute_list, start_date, end_date):
    attribute_str = xyk_common_data_processing.get_attribute_str(attribute_list)
    str1 = "select S_INFO_WINDCODE, TRADE_DT, " + attribute_str + " from " + table_name + " " \
                "where TRADE_DT >= '%s' and TRADE_DT <= '%s' order by TRADE_DT, S_INFO_WINDCODE" % (start_date, end_date)
    rows = DB.select(str1)
    #print rows[0]
    RDF_total_dict = {}
    RDF_total_key_list = []
    i = 0
    while i < len(rows):
        RDF_total_dict[str(rows[i][0]), str(rows[i][1])] = []
        j = 0
        while j < len(attribute_list):
            if rows[i][j + 2] != None:
                RDF_total_dict[str(rows[i][0]), str(rows[i][1])].append(float(rows[i][j + 2]))
            else:
                RDF_total_dict[str(rows[i][0]), str(rows[i][1])].append("")
            j += 1
        RDF_total_key_list.append([str(rows[i][0]), str(rows[i][1])])
        i += 1
    return RDF_total_dict, RDF_total_key_list

'''
***在WIND-DB中查询并获取全部股票复权后收盘价与昨日收盘价（小list每日一个）（字典形式），如果数量不足或者当前处于停盘，返回空***
'''
def get_all_stock_closing_price(start_date, end_date, minimum_amount):
    str1 = "select S_INFO_WINDCODE, TRADE_DT, S_DQ_ADJCLOSE, S_DQ_ADJPRECLOSE from AShareEODPrices " \
                "where TRADE_DT >= '%s' and TRADE_DT <= '%s' order by S_INFO_WINDCODE, TRADE_DT" % (start_date, end_date)
    rows = DB.select(str1)
    RDF_close_price_total_dict = {}
    RDF_close_price_total_stock_list = []
    null_list = []
    i = 0
    while i < len(rows):
        '''初始化'''
        if i == 0:
            RDF_close_price_temp_list = []
        elif rows[i][0] != rows[i - 1][0]:
            if str(rows[i - 1][1]) != end_date:
                RDF_close_price_total_dict[str(rows[i - 1][0])] = null_list
            elif len(RDF_close_price_temp_list) < minimum_amount:
                RDF_close_price_total_dict[str(rows[i - 1][0])] = null_list
            else:
                RDF_close_price_total_dict[str(rows[i - 1][0])] = RDF_close_price_temp_list
                RDF_close_price_total_stock_list.append(str(rows[i - 1][0]))
            RDF_close_price_temp_list = []
        else:
            pass
        '''添加日期序列'''
        RDF_close_price_temp_list.append([str(rows[i][1]), float(rows[i][2]), float(rows[i][3])])
        i += 1
    '''还差最后一组未加入'''
    if str(rows[i - 1][1]) != end_date:
        RDF_close_price_total_dict[str(rows[i - 1][0])] = null_list
    elif len(RDF_close_price_temp_list) < minimum_amount:
        RDF_close_price_total_dict[str(rows[i - 1][0])] = null_list
    else:
        RDF_close_price_total_dict[str(rows[i - 1][0])] = RDF_close_price_temp_list
        RDF_close_price_total_stock_list.append(str(rows[i - 1][0]))
                                   
    return RDF_close_price_total_dict, RDF_close_price_total_stock_list
    
'''
***计算出全部股票的ROR字典（小list分别是日期和ROR）***
'''
def get_all_stock_ROR(start_date, end_date, minimum_amount):
    (RDF_close_price_total_dict, RDF_close_price_total_stock_list) = get_all_stock_closing_price(start_date, end_date, minimum_amount)
    RDF_ROR_total_dict = {}
    i = 0
    while i < len(RDF_close_price_total_stock_list):
        RDF_close_price_temp_list = RDF_close_price_total_dict[RDF_close_price_total_stock_list[i]]
        RDF_ROR_temp_list = []
        RDF_ROR_temp_list_date = []
        RDF_ROR_temp_list_ROR = []
        j = 0
        while j < len(RDF_close_price_temp_list):
            RDF_ROR_temp_list_date.append(RDF_close_price_temp_list[j][0])
            RDF_ROR_temp_list_ROR.append(RDF_close_price_temp_list[j][1] / RDF_close_price_temp_list[j][2] - 1.0)
            j += 1
        RDF_ROR_temp_list.append(RDF_ROR_temp_list_date)
        RDF_ROR_temp_list.append(RDF_ROR_temp_list_ROR)
        RDF_ROR_total_dict[RDF_close_price_total_stock_list[i]] = RDF_ROR_temp_list
        i += 1
    
    return RDF_ROR_total_dict, RDF_close_price_total_stock_list

'''
***在WIND-DB中查询并获取全部股票横截面数据（字典形式）***
'''
def get_all_stock_slide_value(table_name, indicator, date):
    str1 = "select S_INFO_WINDCODE, " + indicator + " from " + table_name + " "\
                "where TRADE_DT = '%s' order by S_INFO_WINDCODE" % (date)
    rows = DB.select(str1)
    RDF_total_dict = {}
    RDF_total_stock_list = []
    i = 0
    while i < len(rows):
        if rows[i][0] != None and rows[i][1] != None:
            RDF_total_dict[str(rows[i][0])] = float(rows[i][1])
            RDF_total_stock_list.append(str(rows[i][0]))
        i += 1
    return RDF_total_dict, RDF_total_stock_list

'''
***在WIND-DB中查询并获取全部股票时间序列数据（小list分别是日期和所求数据）（字典形式），如果数量不足或者当前处于停盘，返回空***
'''
def get_all_stock_time_series(table_name, indicator, start_date, end_date, minimum_amount):
    str1 = "select S_INFO_WINDCODE, TRADE_DT, " + indicator + " from " + table_name + " "\
                "where TRADE_DT >= '%s' and TRADE_DT <= '%s' order by S_INFO_WINDCODE, TRADE_DT" % (start_date, end_date)
    rows = DB.select(str1)
    RDF_total_dict = {}
    RDF_total_stock_list = []
    null_list = []
    i = 0
    while i < len(rows):
        '''初始化'''
        if i == 0:
            RDF_temp_list = []
        elif rows[i][0] != rows[i - 1][0]:
            if str(rows[i - 1][1]) != end_date:
                RDF_total_dict[str(rows[i - 1][0])] = null_list
            elif len(RDF_temp_list) < minimum_amount:
                RDF_total_dict[str(rows[i - 1][0])] = null_list
            else:
                RDF_total_dict[str(rows[i - 1][0])] = xyk_common_data_processing.exchange_sequence(RDF_temp_list)
                RDF_total_stock_list.append(str(rows[i - 1][0]))
            RDF_temp_list = []
        else:
            pass
        '''添加日期序列'''
        if rows[i][1] != None and rows[i][2] != None:
            RDF_temp_list.append([str(rows[i][1]), float(rows[i][2])])
        i += 1
    '''还差最后一组未加入'''
    if str(rows[i - 1][1]) != end_date:
        RDF_total_dict[str(rows[i - 1][0])] = null_list
    elif len(RDF_temp_list) < minimum_amount:
        RDF_total_dict[str(rows[i - 1][0])] = null_list
    else:
        RDF_total_dict[str(rows[i - 1][0])] = xyk_common_data_processing.exchange_sequence(RDF_temp_list)
        RDF_total_stock_list.append(str(rows[i - 1][0]))
    
    return RDF_total_dict, RDF_total_stock_list
    
'''
***在WIND-DB中查询并获取日期和指数收盘点数，分别是一个list，如果数量不足返回空***
'''
def get_date_and_index_closing_price(index_code, table, start_date, end_date, minimum_amount):
    str1 = "select TRADE_DT, S_DQ_CLOSE from " + table + " "\
                "where TRADE_DT >= '%s' and TRADE_DT <= '%s' and S_INFO_WINDCODE = '%s'" % (start_date, end_date, index_code)
    rows = DB.select(str1)
    RDF_close_price_list = []
    #print rows
    if len(rows) < minimum_amount:
        return RDF_close_price_list
    else:
        for one in rows:
            temp_list = []
            temp_list.append(str(one[0]))
            temp_list.append(float(one[1]))
            RDF_close_price_list.append(temp_list)
        RDF_close_price_list2 = sorted(RDF_close_price_list, key=lambda x:x[0])
        RDF_close_price_list3 = xyk_common_data_processing.exchange_sequence(RDF_close_price_list2)
        return RDF_close_price_list3
        
'''
***使用WIND-DB计算出类沪深300指数的成分股权重（小list分别是成分股和权重）***
***每月取前半截进行的调整权重，如没有则向前推***
'''
def get_index_weight(last_date, index):
    begin_date = str(int(last_date) - 10000)
    true_due_date = last_date[:6] + "15"
    str1 = "select TRADE_DT, S_CON_WINDCODE, I_WEIGHT from AIndexHS300FreeWeight "\
                "where TRADE_DT > '%s' and TRADE_DT <= '%s' and S_INFO_WINDCODE = '%s' order by TRADE_DT desc, S_CON_WINDCODE asc" % (begin_date, true_due_date, index)
    rows = DB.select(str1)
    weight_list = []
    stock_list = []
    i = 0
    while i < len(rows):
        if i == 0:
            curr_date = str(rows[i][0])
        if str(rows[i][0]) == curr_date:
            stock_list.append(str(rows[i][1]))
            weight_list.append(float(rows[i][2]))
        i += 1
    return stock_list, weight_list
        
'''
***在WIND-DB中查询并获取全部股票每日财务数据（字典形式），如果当前处于停盘等异常状态，返回空***
***注意：这里未考虑年报晚于次年一季报公布的情况，如有需要再行调整***
'''
def get_all_stock_daily_financial_data(table_name, attribute_list, date):
    attribute_str = xyk_common_data_processing.get_attribute_str(attribute_list)
    begin_date = str(int(date) - 10000)
    str1 = "select S_INFO_WINDCODE, ANN_DT, REPORT_PERIOD, " + attribute_str + " from " + table_name + " " \
                "where ANN_DT >= '%s' and ANN_DT <= '%s' order by S_INFO_WINDCODE, ANN_DT desc, REPORT_PERIOD desc" % (begin_date, date)
    rows = DB.select(str1)
    RDF_total_dict = {}
    RDF_total_key_list = []
    i = 0
    while i < len(rows):
        if i == 0:
            RDF_total_dict[str(rows[i][0])] = []
            j = 0
            while j < len(attribute_list):
                if rows[i][j + 3] != "" and rows[i][j + 3] != None:
                    #print rows[i][j + 3]
                    RDF_total_dict[str(rows[i][0])].append(float(rows[i][j + 3]))
                else:
                    RDF_total_dict[str(rows[i][0])].append("")
                j += 1
            RDF_total_key_list.append(str(rows[i][0]))
        elif rows[i][0] != rows[i - 1][0]:
            RDF_total_dict[str(rows[i][0])] = []
            j = 0
            while j < len(attribute_list):
                if rows[i][j + 3] != "" and rows[i][j + 3] != None:
                    #print rows[i][j + 3]
                    RDF_total_dict[str(rows[i][0])].append(float(rows[i][j + 3]))
                else:
                    RDF_total_dict[str(rows[i][0])].append("")
                j += 1
            RDF_total_key_list.append(str(rows[i][0]))
        else:
            pass
        i += 1
    return RDF_total_dict, RDF_total_key_list

'''
***在WIND-DB中查询并获取所有股票的资产负债表所需字段序列（字典形式）***
***优先使用合并报表(更正前)，不存在再使用合并报表，无论如何不使用合并报表(调整)，可能数据准确性受一定影响，但这绝对不包含预知未来***
'''
def get_all_stock_balance_sheet_data(date, field):
    start_date = str(int(date) - 10000)
    str1 = "select S_INFO_WINDCODE, %s from AShareBalanceSheet " \
            "where ANN_DT >= '%s' and ANN_DT < '%s' and (STATEMENT_TYPE = '%s' or STATEMENT_TYPE = '%s') "\
            "order by S_INFO_WINDCODE asc, REPORT_PERIOD desc, STATEMENT_TYPE desc" % (field, start_date, date, "408005000", "408001000")
    rows = DB.select(str1)
    RDF_total_dict = {}
    RDF_total_stock_list = []
    i = 0
    while i < len(rows):
        if rows[i][1] != None:
            if RDF_total_dict.has_key(str(rows[i][0])) == True:
                pass
            else:
                RDF_total_dict[str(rows[i][0])] = float(rows[i][1])
                RDF_total_stock_list.append(str(rows[i][0]))
        i += 1
    return RDF_total_dict, RDF_total_stock_list

'''
***在WIND-DB中查询并获取所有股票的利润表所需字段序列（字典形式）***
***优先使用合并报表(更正前)，不存在再使用合并报表，无论如何不使用合并报表(调整)，可能数据准确性受一定影响，但这绝对不包含预知未来***
***可能只有2期（半年报和年报）***
'''
def get_all_stock_Income_ttm_data(date, field):
    start_date = str(int(date) - 40000)
    str1 = "select S_INFO_WINDCODE, REPORT_PERIOD, %s from AShareIncome " \
                "where ANN_DT >= '%s' and ANN_DT < '%s' and (STATEMENT_TYPE = '%s' or STATEMENT_TYPE = '%s') "\
                "order by S_INFO_WINDCODE asc, REPORT_PERIOD desc, STATEMENT_TYPE desc" % (field, start_date, date, "408005000", "408001000")
    rows = DB.select(str1)
    RDF_total_dict = {}
    RDF_total_stock_list = []
    null_list = []
    i = 0
    while i < len(rows):
        '''初始化'''
        if i == 0:
            RDF_temp_list = []
        elif rows[i][0] != rows[i - 1][0]:
            if len(RDF_temp_list) < 2:
                RDF_total_dict[str(rows[i - 1][0])] = null_list
            else:
                cal_temp_list = []
                j = 0
                while j < len(RDF_temp_list):
                    if j == 0:
                        temp_ym = RDF_temp_list[j][0][:6]
                        temp_ym_af  = str(int(temp_ym) - 100)
                        is_match_a = 0
                        is_match_b = 0
                        is_match = 0
                        k = 0
                        while k < len(RDF_temp_list):
                            if RDF_temp_list[k][0][:6] == temp_ym_af:
                                is_match_a = 1
                            k += 1    
                        k = 0
                        while k < len(RDF_temp_list):
                            if RDF_temp_list[k][0][:6] == (temp_ym_af[:4] + "12"):
                                is_match_b = 1
                            k += 1          
                        if is_match_a == 1 and is_match_b == 1:
                            is_match = 1
                            cal_temp_list.append(RDF_temp_list[j][1])
                            cal_temp_list.append(RDF_temp_list[j][0])
                            if RDF_temp_list[j][0][4:6] == "12":
                                cal_temp_list.append(0.0)
                                cal_temp_list.append(0.0)
                    elif is_match == 0:
                        is_match_a = 0
                        is_match_b = 0
                        temp_ym = RDF_temp_list[j][0][:6]
                        temp_ym_af  = str(int(temp_ym) - 100)
                        k = j
                        while k < len(RDF_temp_list):
                            if RDF_temp_list[k][0][:6] == temp_ym_af:
                                is_match_a = 1
                            k += 1    
                        k = j
                        while k < len(RDF_temp_list):
                            if RDF_temp_list[k][0][:6] == (temp_ym_af[:4] + "12"):
                                is_match_b = 1
                            k += 1          
                        if is_match_a == 1 and is_match_b == 1:
                            is_match = 1
                            cal_temp_list.append(RDF_temp_list[j][1])
                            cal_temp_list.append(RDF_temp_list[j][0])
                            if RDF_temp_list[j][0][4:6] == "12":
                                cal_temp_list.append(0.0)
                                cal_temp_list.append(0.0)
                    elif RDF_temp_list[j][0][4:6] == "12" and len(cal_temp_list) == 2:
                        cal_temp_list.append(RDF_temp_list[j][1])
                    elif RDF_temp_list[j][0][4:6] == cal_temp_list[1][4:6] and len(cal_temp_list) == 3:
                        cal_temp_list.append(RDF_temp_list[j][1])
                        break
                    else:
                        pass
                    j += 1
                if len(cal_temp_list) != 4:
                    RDF_total_dict[str(rows[i - 1][0])] = null_list
                else:
                    RDF_total_dict[str(rows[i - 1][0])] = cal_temp_list[0] + cal_temp_list[2] - cal_temp_list[3]
                    RDF_total_stock_list.append(str(rows[i - 1][0]))
            RDF_temp_list = []
        else:
            pass
        '''添加日期序列'''
        if rows[i][2] != None:
            RDF_temp_list.append([str(rows[i][1]), float(rows[i][2])])
        i += 1
    '''还差最后一组未加入'''
    if len(RDF_temp_list) < 2:
        RDF_total_dict[str(rows[i - 1][0])] = null_list
    else:
        cal_temp_list = []
        j = 0
        while j < len(RDF_temp_list):
            if j == 0:
                temp_ym = RDF_temp_list[j][0][:6]
                temp_ym_af  = str(int(temp_ym) - 100)
                is_match_a = 0
                is_match_b = 0
                is_match = 0
                k = 0
                while k < len(RDF_temp_list):
                    if RDF_temp_list[k][0][:6] == temp_ym_af:
                        is_match_a = 1
                    k += 1    
                k = 0
                while k < len(RDF_temp_list):
                    if RDF_temp_list[k][0][:6] == (temp_ym_af[:4] + "12"):
                        is_match_b = 1
                    k += 1          
                if is_match_a == 1 and is_match_b == 1:
                    is_match = 1
                    cal_temp_list.append(RDF_temp_list[j][1])
                    cal_temp_list.append(RDF_temp_list[j][0])
                    if RDF_temp_list[j][0][4:6] == "12":
                        cal_temp_list.append(0.0)
                        cal_temp_list.append(0.0)
            elif is_match == 0:
                is_match_a = 0
                is_match_b = 0
                temp_ym = RDF_temp_list[j][0][:6]
                temp_ym_af  = str(int(temp_ym) - 100)
                k = j
                while k < len(RDF_temp_list):
                    if RDF_temp_list[k][0][:6] == temp_ym_af:
                        is_match_a = 1
                    k += 1    
                k = j
                while k < len(RDF_temp_list):
                    if RDF_temp_list[k][0][:6] == (temp_ym_af[:4] + "12"):
                        is_match_b = 1
                    k += 1          
                if is_match_a == 1 and is_match_b == 1:
                    is_match = 1
                    cal_temp_list.append(RDF_temp_list[j][1])
                    cal_temp_list.append(RDF_temp_list[j][0])
                    if RDF_temp_list[j][0][4:6] == "12":
                        cal_temp_list.append(0.0)
                        cal_temp_list.append(0.0)
            elif RDF_temp_list[j][0][4:6] == "12" and len(cal_temp_list) == 2:
                cal_temp_list.append(RDF_temp_list[j][1])
            elif RDF_temp_list[j][0][4:6] == cal_temp_list[1][4:6] and len(cal_temp_list) == 3:
                cal_temp_list.append(RDF_temp_list[j][1])
                break
            else:
                pass
            j += 1
        if len(cal_temp_list) != 4:
            RDF_total_dict[str(rows[i - 1][0])] = null_list
        else:
            RDF_total_dict[str(rows[i - 1][0])] = cal_temp_list[0] + cal_temp_list[2] - cal_temp_list[3]
            RDF_total_stock_list.append(str(rows[i - 1][0]))
    return RDF_total_dict, RDF_total_stock_list

'''
***在WIND-DB中查询并获取全部银行间拆放利率***
'''
def get_IBOR_dict(IBOR_type, windcode, count_length = 1):
    if IBOR_type == "SHIBOR":
        table_name = "shiborprices"
    elif IBOR_type == "TIBOR":
        table_name = "tiborprice"
    elif IBOR_type == "EURIBOR":
        table_name = "euriborprice"
    elif IBOR_type == "HIBOR":
        table_name = "hiborprices"
    elif IBOR_type == "LIBOR":
        table_name = "liborprices"
    else:
        print "This IBOR type is unknown!"
    year_length = 360
    str1 = "select TRADE_DT, B_INFO_RATE from " + table_name + " \
                where S_INFO_WINDCODE = '%s' order by TRADE_DT" % (windcode)
    rows = DB.select(str1)
    RDF_total_dict = {}
    i = 0
    while i < len(rows):
        if rows[i][0] != None and rows[i][1] != None:
            annual_rate = float(rows[i][1])
            true_rate = annual_rate * float(count_length) / float(year_length) / 100.0
            RDF_total_dict[str(rows[i][0])] = true_rate
        i += 1
    return RDF_total_dict

#SHIBOR_dict = get_IBOR_dict("SHIBOR", "SHIBORON.IR")