import pandas as pd
import pymysql
from sqlalchemy import types, create_engine
from sqlalchemy.types import NVARCHAR, Text, TEXT
import os
import re
import numpy as np
from elasticsearch import Elasticsearch

for dirpath, dnames, fnames in os.walk("/home/abhilash/All_reports_30092020"):
    seed_filename = 'All_Reports_20200930_1-4 Family Residential Net Loans and Leases.csv'
    global_dataframe = pd.read_csv(os.path.join(dirpath, seed_filename), header=0, low_memory=False)
    list_of_columns = []
    for f in fnames:
        if (f.endswith(".csv")  and (f is not "Readme.txt") and (f is not "All_Reports_20200930_1-4 Family Residential Net Loans and Leases.csv")):
            print(f)
            df = pd.read_csv(os.path.join(dirpath, f), header=0, low_memory=False)
            print(df.shape)
            list_of_columns.extend(list(df.columns))
            global_dataframe = pd.merge(df, global_dataframe, how='outer',on='fed_rssd', validate="one_to_one")
global_dataframe = global_dataframe.loc[:,~global_dataframe.columns.duplicated()]
print(global_dataframe)
print(list_of_columns)
# global_dataframe.to_json("combined_data_validated.json", orient='records')
global_dataframe.to_csv('call_reports_combined_30092020.csv')