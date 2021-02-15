import pandas as pd
import pymysql
from sqlalchemy import types, create_engine
from sqlalchemy.types import NVARCHAR, Text, TEXT
import os
import re
import numpy as np
from elasticsearch import Elasticsearch

for dirpath, dnames, fnames in os.walk("/home/abhilash/EIDL_loans_updated_dec_4/120120_EIDL_Data"):
    seed_filename = '01 EIDL through 111520.csv'
    global_dataframe = pd.read_csv(os.path.join(dirpath, seed_filename), header=0, low_memory=False, error_bad_lines=False)
    list_of_columns = []
    for f in fnames:
        if (f.endswith(".csv")  and (f is not "Readme.txt") and (f is not "01 EIDL through 111520.csv")):
            print(f)
            df = pd.read_csv(os.path.join(dirpath, f), header=0, low_memory=False, error_bad_lines=False)
            print(df.shape)
            list_of_columns.extend(list(df.columns))
            global_dataframe = pd.concat([df, global_dataframe], ignore_index=True)
print(global_dataframe)
print(list_of_columns)
global_dataframe.to_json("combined_EIDL_data_validated.json", orient='records')
#global_dataframe.to_csv('call_EIDL_combined.csv')