#!/usr/bin/python
import json
import config
import argparse
from pymongo import MongoClient
import os
from glob import glob
mongo_client = MongoClient()


def insert_into_mongo(documents_array):
    try:
        response = collection.insert_many(documents_array)
        return response
    except Exception as e:
        print(documents_array)
        print(e)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='extract from json')
    parser.add_argument('-d', '--database', help='database for mongodb')
    parser.add_argument('-c', '--collection', help='collection for mongodb')
    parser.add_argument('-f', '--file', help='file to process')
    # parser.add_argument('-dir', '--directory', help='directory to process')
    args = parser.parse_args()

    database = mongo_client[args.database]
    collection = database[args.collection]
    if collection.drop(): 
        print('Collection dropped successfully') 
    else: 
        print('Collection not present')
    data = []
    # sub_directories = [x[0] for x in os.walk(args.directory)]
    # for directory in sub_directories:
    #     for json_file in glob(os.path.join(directory, '*.txt')):
            # with open(args.file) as f:
    with open(args.file) as f:
        for line in f:
            data.append(json.loads(line))
            if len(data) > 10000:
                insert_into_mongo(data)
                data = []
                print('Inserted 10000 documents')
            else:
                continue
        if len(data) > 0:
            insert_into_mongo(data)
