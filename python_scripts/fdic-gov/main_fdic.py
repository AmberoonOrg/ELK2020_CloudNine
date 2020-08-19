import config
from pymongo import MongoClient
import urllib3
import requests
import logging
from pprint import pprint
logging.basicConfig(level=logging.DEBUG)

mongo_client = MongoClient()
database = mongo_client['fidc']
collection = database['locations_test']


def url_constructor(element):
    url = "https://banks.data.fdic.gov/api/{0}".format(element)
    return url


def fetch_data(payload, url):
    headers = {'accept': 'application/json'}
    json = requests.get(url, params=payload, headers=headers)
    return json.json()


def extract_data(json):
    documents_array = []
    # pprint(json)
    for data in json['data']:
        documents_array.append(data['data'])
    return documents_array


def insert_into_mongo(documents_array):
    response = collection.insert_many(documents_array)
    return response


if __name__ == "__main__":
    data_type = "institutions"
    payload = {
        'fields': 'ZIP,OFFDOM,CITY,COUNTY,STNAME,STALP,NAME,ACTIVE,CERT,CBSA,ASSET,NETINC,DEP,DEPDOM,ROE,ROA,DATEUPDT,OFFICES',
        'sort_by': 'OFFICES',
        'format': 'json',
        'download': 'false'
    }
    for i in range(1, 20):
        payload['limit'] = (i)*10000
        payload['offset'] = (i-1)*10000
        json = fetch_data(payload, url_constructor(data_type))
        documents_array = extract_data(json)
        insert_into_mongo(documents_array)
        print("Done")
        break
