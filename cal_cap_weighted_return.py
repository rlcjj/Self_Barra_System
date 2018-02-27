# -*- coding: utf-8 -*-
"""
Created on Fri Jan 26 14:45:15 2018

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
import db_interaction

start_date = "20070114"
end_date = "20070115"
weighted_type = "liquid"
index_name = "hs300"

'''
***获取成分股数据***
'''
components_dict = db_interaction.get_data_commonly("daily_index_components_" + index_name, ["stock_id"], ["curr_date"], 1, 0, 0)
for date in components_dict.keys():
    components_dict[date] = xyk_common_data_processing.change_stock_format("no_tail", "with_tail", components_dict[date], 1)
#daily_date_list = xyk_common_wind_db_interaction.get_calendar(start_date, end_date, 0)
#
#'''
#***获得全部出现过的股票代码序列，用于查询行情和停牌信息***
#'''
#total_stock_list = xyk_common_data_processing.get_all_element_from_dict(components_dict)
#
#'''
#***从行情序列中查询，并检索停牌信息，剔除成分股中停牌的股票***
#'''
#hq_dict = db_interaction.get_daily_data_dict(start_date, end_date, "daily_stock_technical", ['liquid_MV', 'close'], total_stock_list, 0)
suspension_dict = db_data_pre_treat.get_stock_suspension_dict(start_date, end_date, 1)
components_dict = xyk_common_data_processing.get_dict_difference(components_dict, suspension_dict)

'''
***计算加权的return***
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