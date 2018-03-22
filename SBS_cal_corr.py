# -*- coding: utf-8 -*-
"""
Created on Tue Feb 27 14:39:46 2018

@author: xiyuk
"""

import math
import numpy as np
import pandas as pd
from datetime import datetime
import datetime as dt
import db_interaction
import xyk_common_data_processing

#des_data_df = db_interaction.get_data_df("daily_stock_descriptors_unified_pretreated", ["Long_Momentum", "Medium_Momentum", "Short_Momentum"], ["stock_id", "curr_date"])
des_data_df = db_interaction.get_data_df("daily_stock_factors_all", ["Momentum", "Reversal", "ROR"], ["stock_id", "curr_date"])
corr = des_data_df.corr()