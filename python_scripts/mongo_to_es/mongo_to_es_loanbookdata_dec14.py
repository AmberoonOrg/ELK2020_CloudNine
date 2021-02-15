from pymongo import MongoClient
import time
from elasticsearch import Elasticsearch, helpers
import json
from bson.json_util import dumps
from bson import json_util
import datetime

mongo_client = MongoClient()
es_client = Elasticsearch(['http://10.168.0.2:9200'], http_auth=('elastic', 'amberoonqwerty@456'))
db = mongo_client['loanbookdatadec14']
db2 = mongo_client['us_zip_code']
collection_zip_code = db2.us_zip_code
db3 = mongo_client['niacs_lookup']
collection_naics = db3.data_dictionary
collections = db.collection_names()
print(collections)

for collection_name in collections:
    collection = db[collection_name]
    actions = []
    es_client.indices.delete(index=collection_name+ '', ignore=[400, 404])
    print('Collection {}'. format(collection_name))
    for document in collection.find():
        document['system_refresh_time'] = datetime.datetime.now().isoformat()
        response = collection_zip_code.find_one({"fields.zip": str(document['Note Collateral Description 4 -Zip'])})
        if response:
            document['location'] = {
                "lat": response['fields']['latitude'],
                "lon": response['fields']['longitude']
            }
        print(document['NAICS'])
        response_naics = collection_naics.find_one({"Code": document['NAICS']})
        print(response_naics)
        if response_naics:
            print(response_naics['Title'])
            document['naics_title'] = response_naics['Title']
        body = {
            "_index": 'loan_book_data_dec14',
            "body": json.loads(json.dumps(document, default=json_util.default))
        }
        actions.append(body)
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
print('done')

