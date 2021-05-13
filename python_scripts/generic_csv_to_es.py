from elasticsearch import Elasticsearch, helpers
import csv
import argparse

parser = argparse.ArgumentParser(description='Process csv file to be ingested into Elasticsearch')
parser.add_argument("--host", help="Elasticsearch host")
parser.add_argument("--index", help="Index name for Elasticsearch host")
parser.add_argument("--csvFile", help="CSV file to be ingested")
args = parser.parse_args()


es = Elasticsearch(host = args.host, port = 9200)
csvFile = args.csvFile

with open(csvFile) as f:
    reader = csv.DictReader(f)
    helpers.bulk(es, reader, index=args.index)