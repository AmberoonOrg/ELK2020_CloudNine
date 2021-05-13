# Author: Abhilash Bolla
# Version: 1.0

from elasticsearch import Elasticsearch, helpers
import json
import argparse

def construct_document(key, value):
    document = {}
    document['field'] = key
    document['type'] = value.get('type')
    document['title'] = value.get('title')
    document['description'] = value.get('description')
    return document

parser = argparse.ArgumentParser(description='Process json file for Phoenix data dictionary to be ingested into Elasticsearch')
parser.add_argument("--host", help="Elasticsearch host")
parser.add_argument("--index", help="Index name for Elasticsearch host")
parser.add_argument("--folder", help="CSV file to be ingested")
args = parser.parse_args()


es = Elasticsearch(host = args.host, port = 9200)
folder = args.folder

for dirpath, dnames, fnames in os.walk(folder):
    for f in fnames:
        if (f.endswith(".json")):
            print(f)
            with open(f) as jsonFile:
                json = json.load(jsonFile)
                documents = []
                for key, value in json["properties"].iteritems():
                    document = construct_document(key, value)
                    documents.append(document)
                helpers.bulk(es, documents, index=args.index)