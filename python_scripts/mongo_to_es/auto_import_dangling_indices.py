import os
import requests

indices = requests.get('http://localhost:9200/_dangling')
indices_json = indices.json()
indices = indices_json['dangling_indices']

for index in indices:
    print("Processing index:", index["index_name"])
    requests.post("http://localhost:9200/_dangling/" + index['index_uuid'] +"?accept_data_loss=true&pretty")
print("Done")