"""
This script is written to initialize the newly added field 'sponsored_date'. We are adding this field to keep track of the date that a sponsor/co-sponsor is added to the bill
We can only keep track of this using our weekly data update. Therefore, for all the bills before this implementation (date -- 2021-11-10) the sponsored date is defaulted to the 
bill introduction date
"""

from elasticsearch import Elasticsearch
from src.utils.project_constants import BILL_META_INDEX
from src.utils.general import get_elasticsearch_conn
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
fh = logging.FileHandler('../../logs/initializing_sponsor_start_end_dates_es.log', mode='w')
fh.setFormatter(formatter)
logger.addHandler(fh)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)



es = get_elasticsearch_conn("../../conf/local/credentials.yaml")


def update_and_reindex(es, res):
    """Update the sponsors field with the date attribute and reindex"""
    
    bill_docs = [x['_source'] for x in res['hits']['hits']]
    
    for bill in bill_docs:
        logging.info(f'Processing bill {bill["bill_id"]}')
        try:
            intro_date = [x['date'] for x in bill['progress'] if x['event']==1][0]
        # There are a few cases where the introduced event is not present in data
        # But these bills are introduced and passes/dies on the same day. 
        # Therefore, has little bearing on the models we build
        # So, just filling in a date to keep structure consistent
        except IndexError:
            if not bill['history']:
                intro_date = '1970-01-01'
                logging.warning('No progress or event history found. Setting 1970-01-01 as the sponsor date')
            else:
                event_dates = [x['date'] for x in bill['history']]
                intro_date = sorted(event_dates)[0]         
        
        logging.info(f'Setting {intro_date} as sponsored_date')
        for sponsor in bill['sponsors']:
            # We changed the field name to "sposnor_start_date"
            if 'sponsored_date' in sponsor: 
                sponsor.pop('sponsored_date')
            sponsor['sponsor_start_date'] = intro_date
            sponsor['sponsor_end_date'] = 'null'
        # reindexing
        es.index(index=BILL_META_INDEX, id=bill['bill_id'], body=bill)
    

query = {'query': {'match_all': {}}}

res = es.search(index=BILL_META_INDEX, body=query, scroll='1m', size=100)
scroll_id = res.get('_scroll_id')
update_and_reindex(es, res)

stop = False
while not stop:
    res = es.scroll(scroll_id=scroll_id, scroll='1m')
    scroll_id = res.get('_scroll_id')

    if (res['hits']['total']['value']==0) or (len(res['hits']['hits']) == 0):
        stop = True
        continue

    update_and_reindex(es, res)





