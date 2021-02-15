import pandas as pd
import pymysql
from sqlalchemy import types, create_engine
from sqlalchemy.types import NVARCHAR, Text, TEXT
import os
import re
import numpy as np

MYSQL_USER 		= 'root'
MYSQL_PASSWORD 	= 'amberoon@456'
MYSQL_HOST_IP 	= '127.0.0.1'
MYSQL_PORT		= '3306'
MYSQL_DATABASE	= 'fdic_call_reports'

#engine = create_engine('')
chunksize = 500
engine = create_engine('mysql+pymysql://'+MYSQL_USER+':'+MYSQL_PASSWORD+'@'+MYSQL_HOST_IP+':'+MYSQL_PORT+'/'+MYSQL_DATABASE, echo=False)
pattern = ('FFIEC CDR Call (Bulk|Schedule) (\w+) (\d{8})(\(\d+ of \d+\))?.txt')

def get_table_name(filename):
    match = re.compile(pattern).match(filename)
    if not match or 3 > len(match.groups()):
        print('bad filename: {}'.format(filename))
    else:
        return match.group(2)

for dirpath, dnames, fnames in os.walk("/home/abhilash/ffiec_june2020"):
    global_column_dictionary = {}
    sum = 0
    for f in fnames:
        if (f.endswith(".txt") and f is not "Readme.txt"):
            print(f)
            match = re.compile(pattern).match(f)
            if not match or 3 > len(match.groups()):
                print('bad filename: {}'.format(f))
                continue
            df = pd.read_csv(os.path.join(dirpath, f), sep='\t', header=0, low_memory=False)
            table_name = 'Schedule' + get_table_name(f)
            # get the list of columns in the df into an array
            #loop through it and build a dynamic sql like   " select max(length(<column_name>)) from df"
            print(table_name)
            try:
                df = df[0:1]
                list_of_columns = df.to_dict('records')
                column_dictionary = list_of_columns[0]
                print("Length of columns dictionary:", len(column_dictionary))
                sum += len(column_dictionary)
                global_column_dictionary = {**global_column_dictionary, **column_dictionary}
            except pymysql.DataError as e:
                print(e)
                pass
            except Exception as e:
                print("Inside Exception")
                print(e)
    print("Length of global dictionary:", len(global_column_dictionary))
    with open('columns.csv', 'w') as f:
        for key in global_column_dictionary.keys():
            f.write("%s,%s\n"%(key,global_column_dictionary[key]))