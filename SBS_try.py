# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 18:41:26 2018

@author: xiyuk
"""

import math
import numpy as np
import pandas as pd
from datetime import datetime
import datetime as dt
import db_interaction
import db_data_pre_treat
import xyk_common_data_processing

#hs300 = db_interaction.get_data_df("daily_index_components_zz500", ["ZZ500"], ["stock_id", "curr_date"])
#hs300['HS300'] = 0
#hs300['ZZ800'] = 1
#hs300['HS300'] = hs300['HS300'].astype('int')
#hs300['ZZ500'] = hs300['ZZ500'].astype('int')
#hs300['ZZ800'] = hs300['ZZ800'].astype('int')
#hs300_1 = hs300.reset_index()
#stock_id_list = xyk_common_data_processing.change_data_format_with_df("DataFrame", "list", hs300_1['stock_id'])
#stock_id_list = xyk_common_data_processing.change_stock_format("no_tail", "with_tail", stock_id_list, 2)
#hs300_1['stock_id'] = stock_id_list
#hs300_1 = xyk_common_data_processing.delete_none(hs300_1, none_value = None, lower_than_that = 0, exchange = 0, how = "any", thresh = None)
#hs300_list = xyk_common_data_processing.change_data_format_with_df("DataFrame", "list of lists", hs300_1)
##db_interaction.insert_df_commonly("daily_index_components", hs300, index_name_list = ["stock_id", "curr_date"], attribute_name_list = ["HS300", "ZZ500", "ZZ800"])
#db_interaction.insert_attributes_commonly("daily_index_components", hs300_list, ["stock_id", "curr_date", "ZZ500", "HS300", "ZZ800"], ["ZZ500", "HS300", "ZZ800"])

start_date = "20050930"
end_date = "20171231"

a_stock = db_data_pre_treat.get_normal_stocklist_dict(start_date, end_date, year = 0, month = 3)

#all_stock_list = []
#
#for date in a_stock.keys():
#    for stock in a_stock[date]:
#        all_stock_list.append([stock, unicode(date), 1])
#        
#print len(all_stock_list) / 100000
#
#db_interaction.insert_attributes_commonly("daily_index_components", all_stock_list, ["stock_id", "curr_date", "No_ST"], ["No_ST"])

#all_a_stock_dict = db_data_pre_treat.get_a_stock_dict(start_date, end_date, 0, 3, 1)

#a300_dict = db_interaction.get_data_commonly("daily_index_components", ["stock_id"], ["curr_date"], one_to_one = 0, where = "HS300 = 1")

