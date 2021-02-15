import json
from elasticsearch import Elasticsearch, helpers
import os
import pymysql, json, datetime
from pymongo import MongoClient

INDEX_NAME = 'ppp_loan_with_call_data_merged_latest_jan7'
es_client = Elasticsearch(['http://10.128.0.5:9200'], http_auth=('elastic', 'amberoonqwerty@456'))
mongo_client = MongoClient()
db2 = mongo_client['us_zip_code']
collection_zip_code = db2.us_zip_code

def construct_document(value):
    temp_document = value
    temp_document['_index'] = INDEX_NAME
    return temp_document

conn = pymysql.connect( user='gerard',
                        password='cV6}lZ9!sQ2^rY9~',
                        host='10.128.0.10',
                        database='callreports',
                        autocommit=1,
                        cursorclass=pymysql.cursors.DictCursor)
                        #auth_plugin='mysql_native_password')

cursor = conn.cursor()

cursor.execute("""select a.LoanAmount, a.BusinessName, a.Address, a.City, a.State, a.Zip, a.NAICSCode, a.BusinessType, a.RaceEthnicity, a.Gender, a.Veteran, a.NonProfit, a.JobsReported, a.DateApproved, a.Lender, a.CD,
       ifnull(b.idrssd,0) as calldata_idrssd, ifnull(b.RSSD9017,'CU') as calldata_lender
  from ppp.ppp_data a
       left outer join ppp.lender l on l.lender=a.lender
       left outer join ppp.rssd_lender_clean b on b.idrssd=l.idrssd""")
result = cursor.fetchall()
#print(f"json: {json.dumps(result)}")

print('Processing ------------------> ')
# print(file.readline())
actions = []
error_count = 0
for element in result:
    document = construct_document(element)
    if document.get('Zip'):
        response = collection_zip_code.find_one({"fields.zip": str(document['Zip'])})
        if response:
            document['location'] = {
                "lat": response['fields']['latitude'],
                "lon": response['fields']['longitude']
            }
            document['city_from_zip'] = response['fields']['city']
            document['state_from_zip'] = response['fields']['state']
        else:
            error_count += 1
    actions.append(document)
    if len(actions) > 5000:
        print("Inserted 5000")
        try:
            helpers.bulk(es_client, actions, chunk_size=1000, request_timeout=200)
        except Exception as e:
            print(e)
        actions = []
try:
    helpers.bulk(es_client, actions, chunk_size=1000, request_timeout=200)
except Exception as e:
    print(e)
print('Done')
