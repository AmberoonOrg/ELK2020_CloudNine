import json
from elasticsearch import Elasticsearch, helpers
import os
from pymongo import MongoClient

mongo_client = MongoClient()
db = mongo_client['us_zip_code']
collection_zip_code = db.us_zip_code
db2 = mongo_client['institutions']
collection_institution = db2.institutions
INDEX_NAME = 'call_reports_master_data_v3_2021'
es_client = Elasticsearch(['http://10.128.0.5:9200'], http_auth=('elastic', 'amberoonqwerty@456'))

def construct_document(key, value):
    temp_document = value
    temp_document['timestamp'] = key
    temp_document['_index'] = INDEX_NAME
    return temp_document

for dirpath, dnames, fnames in os.walk("/home/abhilash/20210125/json"):
    actions = []
    for f in fnames:
        with open(os.path.join(dirpath, f)) as file:
            print('Processing ------------------> ', f)
            data = json.load(file)
            for key, value in data.items():
                for k, v in value.items():
                    document = construct_document(key, v)
                    response = collection_zip_code.find_one({"fields.zip": str(document['Financial Institution Zip Code'])})
                    if response:
                        document['location'] = {
                            "lat": response['fields']['latitude'],
                            "lon": response['fields']['longitude']
                        }
                    response_institution = collection_institution.find_one({"FED_RSSD": document['IDRSSD']})
                    if response_institution:
                        document['is_metropolitan'] = "Yes" if response_institution['CBSA_METRO_FLG'] else "No"
                        num_offices = 0 if response_institution['OFFDOM'] == "" else response_institution['OFFDOM']
                        document['number_of_offices'] = int(str(num_offices).replace(',', ''))
                    actions.append(document)
                    if len(actions) > 100:
                        print("Inserted 100")
                        try:
                            helpers.bulk(es_client, actions, chunk_size=1000, request_timeout=200)
                        except Exception as e:
                            print(e)
                        actions = []
    try:
        helpers.bulk(es_client, actions, chunk_size=10, request_timeout=200)
    except Exception as e:
        print(e)
print('Processing done')
