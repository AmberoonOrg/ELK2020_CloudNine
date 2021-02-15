from pymongo import MongoClient
import time
from elasticsearch import Elasticsearch, helpers
import json
# from bson.json_util import dumps
from bson import json_util

mongo_client = MongoClient()
es_client = Elasticsearch(['http://10.128.0.5:9200'], http_auth=('elastic', 'amberoonqwerty@456'))
db = mongo_client['call_reports_with_ratios']
collections = db.collection_names()
#collections = ["call_reports_june2020"]
print(collections)

for collection_name in collections:
    collection = db[collection_name]
    actions = []
    print('Collection {}'. format(collection))
    count = 0
    for document in collection.find():
        if '' in document:
            del document['']
        body = {
            "_index": "call_reports_with_ratios",
            "_type": "_doc",
            "body": json.loads(json.dumps(document, default=json_util.default))
        }
        actions.append(body)
        if len(actions) > 10:
            helpers.bulk(es_client, actions, chunk_size=1000, request_timeout=200)
            actions = []
            count += 1
            print("Indexed", count)
    helpers.bulk(es_client, actions, chunk_size=1000, request_timeout=200)
print('done')
