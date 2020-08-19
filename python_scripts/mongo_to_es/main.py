from pymongo import MongoClient
import time
from elasticsearch import Elasticsearch, helpers
import json
# from bson.json_util import dumps
from bson import json_util

mongo_client = MongoClient()
es_client = Elasticsearch(['http://10.168.0.2:9200'], http_auth=('elastic', 'amberoonqwerty@456'))
db = mongo_client['alephdata']
collections = db.collection_names()
print(collections)

for collection_name in collections:
    collection = db[collection_name]
    actions = []
    print('Collection {}'. format(collection))
    count = 0
    for document in collection.find():
        body = {
            "_index": collection_name,
            "type": "_doc",
            "body": json.loads(json.dumps(document, default=json_util.default))
        }
        actions.append(body)
        if len(actions) > 5000:
            print("Indexed", count)
            helpers.bulk(es_client, actions, chunk_size=1000, request_timeout=200)
            count += 1
            actions = []
    helpers.bulk(es_client, actions, chunk_size=1000, request_timeout=200)
print('done')
