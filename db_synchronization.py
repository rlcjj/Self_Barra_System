# -*- coding: utf-8 -*-
"""
Created on Wed Feb 21 15:24:37 2018

@author: xiyuk
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import gc

table_name_list = ["daily_stock_descriptors_zz500_unique", "daily_stock_descriptors_pretreated_zz500", "daily_stock_factors_zz500"]

key_list_list = [['stock_id', 'curr_date'], ['stock_id', 'curr_date'], ['stock_id', 'curr_date']]

engine = create_engine('mysql+mysqldb://root:312215@192.168.2.106:3306/style_rotation')
engine2 = create_engine('mysql+mysqldb://mouhaima:312215@localhost:3306/style_rotation')

i = 0
while i < len(table_name_list):
    print i, table_name_list[i]
    data = pd.read_sql_table(table_name_list[i], engine, index_col = key_list_list[i])
    data.to_sql(table_name_list[i], engine2, if_exists = 'append', index_label = key_list_list[i], chunksize = 500)
    del data
    gc.collect()
    i += 1