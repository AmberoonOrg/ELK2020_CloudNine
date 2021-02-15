import pandas as pd
from elasticsearch import Elasticsearch, helpers
import json
from bson import json_util
from bson.json_util import dumps
es_client = Elasticsearch(['http://10.168.0.2:9200'], http_auth=('elastic', 'amberoonqwerty@456'))

def mapp_quarter_to_date(x):
    if "Q1" in x:
        return x.replace("Q1", "/03/31")
    elif "Q2" in x:
        return x.replace("Q2", "/06/30")
    elif "Q3" in x:
        return x.replace("Q3", "/09/30")
    elif "Q4" in x:
        return x.replace("Q4", "/12/31")
    else:
        return None

xl = pd.ExcelFile('/home/abhilash/Ratios_as_asset_size.xlsx')
nrows = xl.book.sheet_by_index(0).nrows
Heading_array = ["Number of Institutions Reporting", "Quarterly Efficiency Ratio", "Quarterly Loss Provision, % of Net Operating Revenue", "Quarterly Net Charge-Offs to Loans and Leases", "Quarterly Loss Provisions, % of Net Charge-Offs", "Core Capital (Leverage) Ratio (PCA)", "Tier 1 Risk-Based Capital Ratio (PCA)*", "Risk-Weighted Assets to Total Assets*"]
title_index = 0
actions = []
for index in range(0, nrows, 7):
    print("Index:",index)
    df = xl.parse(0, skipfooter=(nrows-(index+7)), skiprows=index).dropna(axis=1, how='all')
    # df = df = df.drop(columns=df.columns[[]])
    df.set_index('Asset Size Group', inplace=True)
    df = df.T
    df_modified = df.reset_index()
    df_modified['index'] = df_modified['index'].apply(mapp_quarter_to_date)
    df_modified.columns=df_modified.columns.str.replace(' ','_')
    df_modified.columns=df_modified.columns.str.replace('-','')
    df_modified.columns= df_modified.columns.str.lower()
    resultant_dict = df_modified.to_dict('records')
    for record in resultant_dict:
        record['type'] = Heading_array[title_index]
        record['timestamp'] = record['index']
        del record['index']
        print(record)
        body = {
            "_index": 'ratios_index',
            "_source": json.loads(json.dumps(record, default=json_util.default))
        }
        actions.append(body)
        if len(actions) > 50:
            print("Inserted 50")
            try:
                helpers.bulk(es_client, actions, chunk_size=1000, request_timeout=200)
            except Exception as e:
                print(e)
            actions = []
    title_index += 1
    try:
        helpers.bulk(es_client, actions, chunk_size=1000, request_timeout=200)
    except Exception as e:
        print(e)
    print('done')
