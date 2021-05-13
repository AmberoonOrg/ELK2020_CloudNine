import json
from elasticsearch import Elasticsearch, helpers
import os
from pymongo import MongoClient

mongo_client = MongoClient()
db = mongo_client['us_zip_code']
collection_zip_code = db.us_zip_code
db2 = mongo_client['institutions']
collection_institution = db2.institutions
INDEX_NAME = 'call_reports_master_data_with_ratios_classification_feb_12'
es_client = Elasticsearch(['http://10.128.0.5:9200'], http_auth=('elastic', 'amberoonqwerty@456'))

def construct_document(key, value):
    temp_document = value
    temp_document['timestamp'] = key
    temp_document['_index'] = INDEX_NAME
    return temp_document

def add_peer_group(document):
    peer_group = ""
    asset_size = 0.0
    if document.get('RCFD2170') and document.get('RCON2170'):
        asset_size = 1000*(document.get('RCFD2170') + document.get('RCON2170'))
    elif document.get('RCFD2170'):
        asset_size = 1000*document.get('RCFD2170')
    elif document.get('RCON2170'):
        asset_size = 1000*document.get('RCON2170')
    else:
        peer_group = "N/A"
    
    if(asset_size > 1000000000):
        peer_group = "A) Peer Group 1-4: More than $1B in assets"
    elif (asset_size >300000000 and asset_size< 1000000000):
        peer_group = "B) Peer Group 5: $300 Million - 1 Billion"
    elif(document.get('is_metropolitan') and document.get('number_of_offices')):
        if (document.get('is_metropolitan') == 'Yes' and document.get('number_of_offices') >= 3 and asset_size > 100000000 and asset_size < 300000000):
            peer_group = "C) Peer Group 6: $100M to $300M; 3+ offices; Metro"
        elif (document.get('is_metropolitan') == 'No' and document.get('number_of_offices') >= 3 and asset_size > 100000000 and asset_size < 300000000):
            peer_group = "D) Peer Group 7: $100M to $300M; 3+ offices; Non-Metro"
        elif (document.get('is_metropolitan') == 'Yes' and document.get('number_of_offices') <= 2 and asset_size > 100000000 and asset_size < 300000000):
            peer_group = "E) Peer Group 8; $100M to $300M; 2 or less offices; Metro"
        elif (document.get('is_metropolitan') == 'No' and document.get('number_of_offices') <= 2 and asset_size > 100000000 and asset_size < 300000000):
            peer_group = "F) Peer Group 9; $100M to $300M; 2 or less offices; Non-Metro"
        elif (document.get('is_metropolitan') == 'Yes' and document.get('number_of_offices') >= 3 and asset_size > 50000000 and asset_size < 100000000):
            peer_group = "G) Peer Group 10; $50M to $100M; 3+ offices; Metro"
        elif (document.get('is_metropolitan') == 'No' and document.get('number_of_offices') >= 3 and asset_size > 50000000 and asset_size < 100000000):
            peer_group = "H) Peer Group 11; $50M to $100M; 3+ offices; Non-Metro"
        elif (document.get('is_metropolitan') == 'Yes' and document.get('number_of_offices') <= 2 and asset_size > 50000000 and asset_size < 100000000):
             peer_group = "I) Peer Group 12; $50M to $100M; 2 or less offices; Metro"
        elif (document.get('is_metropolitan') == 'No' and document.get('number_of_offices') <= 2 and asset_size > 50000000 and asset_size < 100000000):
            peer_group = "J) Peer Group 13; $50M to $100M; 2 or less offices; Non-Metro"
        elif (document.get('is_metropolitan') == 'Yes' and document.get('number_of_offices') >= 2 and asset_size < 50000000):
            peer_group = "K) Peer Group 14; Less than $50 Million; 2 + offices; Metro"
        elif (document.get('is_metropolitan') == 'No' and document.get('number_of_offices') >= 2 and asset_size < 50000000):
            peer_group = "L) Peer Group 15; Less than $50 Million; 2 + offices; Non-Metro"
        elif (document.get('is_metropolitan') == 'Yes' and document.get('number_of_offices') == 1 and asset_size < 50000000):
            peer_group = "M) Peer Group 16; Less than $50 Million; 1 office; Metro"
        elif (document.get('is_metropolitan') == 'No' and document.get('number_of_offices') == 1 and asset_size < 50000000):
            peer_group = "N) Peer Group 17; Less than $50 Million; 1 office; Non-Metro"
        else:
            peer_group = "N/A"
    return peer_group






for dirpath, dnames, fnames in os.walk("/home/abhilash/json"):
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
                        document['UBPR Peer Grp Custom'] = add_peer_group(document)
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
