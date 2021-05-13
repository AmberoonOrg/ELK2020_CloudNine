import MySQLdb
from bson import json_util
import json
from elasticsearch import helpers
import elasticsearch

es_client = Elasticsearch(['http://10.168.0.2:9200'], http_auth=('elastic', 'amberoonqwerty@456'))
conn = MySQLdb.connect(
    host="34.94.2.190"
    user="gerard",
    passwd="Yz_*6w{6Y",
    db="call_reports",
    charset='utf8',
    use_unicode=True
)


def get_data(i):
    action = {
            "_index": "call_reports_dictionary_mapping",
            "_type": "sample_data",
            "_source": json.dumps(i, default=json_util.default)
            }
    return action


cur = conn.cursor(cursorclass=MySQLdb.cursors.DictCursor)
print "Executing query"
cur.execute("SELECT * FROM catalogsearch_fulltext")

print "Starting loop"
row = cur.fetchone()
print row
raw_input()
count = 0
actions = []
while row is not None:
    # if count % 1000 == 0 and not count == 0:
    #     helpers.bulk(es, actions, chunk_size=500, request_timeout=50)
    #     actions = []
    #     d = get_data(row)
    #     actions.append(d)
    #     count += 1
    # else:
    #     d = get_data(row)
    #     actions.append(d)
    #     count += 1
    row = cur.fetchone()
    try:
        print int(float(row.get("full_price").replace(',', '')))
    except Exception, e:
        print "-------------------" + row.get("full_price")
        print e
        pass
    # print count
# helpers.bulk(es, actions, chunk_size=500, request_timeout=50)
cur.close()
conn.close()
