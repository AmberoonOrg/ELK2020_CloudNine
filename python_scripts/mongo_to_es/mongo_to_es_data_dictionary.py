from pymongo import MongoClient
import time
from elasticsearch import Elasticsearch, helpers
import json
from bson.json_util import dumps
from bson import json_util
import datetime

mongo_client = MongoClient()
es_client = Elasticsearch(['http://10.168.0.2:9200'], http_auth=('elastic', 'amberoonqwerty@456'))
db = mongo_client['call_reports_data_dictionary_master']
collections = db.collection_names()
print(collections)

for collection_name in collections:
    collection = db[collection_name]
    actions = []
    es_client.indices.delete(index=collection_name+ '', ignore=[400, 404])
    print('Collection {}'. format(collection_name))
    error_count = 0
    for document in collection.find():
        document['system_refresh_time'] = datetime.datetime.now().isoformat()
        body = {
            "_index": 'call_reports_data_dictionary_master',
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
