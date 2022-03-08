"""
    This script updates the elasticsearch index containing bill texts to include the encoded bill text
    The script is necessary if the index does not already include the encoded field. 
"""

import os
import sys
import logging
import json
import pandas as pd

from datetime import date

from src.utils.general import get_db_conn, get_elasticsearch_conn
from src.utils.project_constants import BILL_TEXT_INDEX, PROJECT_FOLDER, LEGISCAN_PLACEHOLDER_DATE, DEFAULT_DATE

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
fh = logging.FileHandler('../../logs/updating_bill_text_index_{}.log'.format(date.today()), mode='w')
fh.setFormatter(formatter)
logger.addHandler(fh)


ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)

legiscan_data_dump_folders = ['legiscan_dump_20200615_processed', 'legiscan_dump_20210709_processed']


def _store_modified_doc(es_conn, fp):
    """Index one bill document given the file name"""

    with open(fp) as json_fp:
        content = json.load(json_fp)

        doc = content["text"]

        if (doc.get('date') == LEGISCAN_PLACEHOLDER_DATE) or (doc.get('date') is None):
                doc['date'] = DEFAULT_DATE

        logging.info('encoded doc size: {}'.format(len(doc.get('doc'))))
        json_object = {
            "bill_id": doc['bill_id'],
            "doc_date": doc['date'],
            "doc": doc.get('doc_decoded'),
            "encoded_doc": doc.get('doc'),
            "doc_id": doc['doc_id'],
            "mime": doc['mime'],
            "mime_id": doc['mime_id'],
            "state_link": doc.get('state_link'),
            "text_size": doc['text_size'],
            "type": doc['type'],
            "type_id": doc['type_id'],
            "url": doc.get('url')
        }

        try:
            es_conn.index(
                index=BILL_TEXT_INDEX, 
                id='{}_{}'.format(doc['bill_id'], doc['doc_id']), 
                body=json.dumps(json_object),
                timeout='30s'
            )
        except ConnectionRefusedError:
            logging.error('Connection error. Could not index {}. Aborting!'.format(doc['doc_id']))
            raise ConnectionRefusedError('Connection refused by elasticsearch instance')
        except ConnectionError:
            logging.error('Connection error. Could not index {}. Aborting!'.format(doc['doc_id']))
            raise ConnectionError('Connection refused by elasticsearch instance')
        except Exception as e:
            logging.warning('Could not index {} due to exception {}'.format(doc['doc_id'], e) )
            raise ValueError('Aborting!')


def process_one_round(es, db, hits, not_found_list):
    """ Processing one batch of results fetched from elasticsearch """
    
    for doc in hits:
        id = doc['_id']
        id = id.split('_')

        # There are some documents in the index with the wrong ID
        if (len(id) < 2):
            continue
        elif (id[0] == id[1]):
            continue

        
        
        doc=doc['_source']
        # print(doc.keys())
        bill_id = doc['bill_id']
        doc_id = doc['doc_id']
        text_size = doc['text_size']

        # We only proceed if the encoded_doc is not already there
        if doc.get('encoded_doc') is not None:
            logging.warning(f'Encoded doc already added. Skipping {doc_id}')
            continue

        logging.info(f'Correct document {id} with size {text_size}')

        q = f'select state, session_id from clean.bills b where bill_id={bill_id}'
        d = pd.read_sql(q, db).to_dict('records')[0]
        found = False
        for dump in legiscan_data_dump_folders:
            
            state_folder = os.path.join(PROJECT_FOLDER, dump, d['state'])

            session_folder = [x for x in os.listdir(state_folder) if str(d['session_id']) in x]

            if session_folder:
                session_folder = session_folder[0]
                text_folder = os.path.join(state_folder, session_folder, 'text')

                fp = os.path.join(text_folder, str(doc_id) + '.json')
                logging.info(fp)

                try:
                    _store_modified_doc(es, fp)
                    found = True
                    break
                except Exception as e:
                    logging.warning(e)
                
        if not found:
            logging.warning(f'Could not find the json file for the doc with doc id {doc_id}')
            not_found_list.append({'bill_id': bill_id, 'doc_id': doc_id})


def update_index(es, db, query_size=100):
    """
        For all documents in the index, add the encoded_doc field.
        args:
            es: elasticsearch connection
    """

    # we need to fetch all the documents in the index
    query = {
        "query": {
            "match_all": {}
        }
    }
    not_found_list = list()
    res = es.search(index=BILL_TEXT_INDEX, body=query, scroll='5m', size=query_size, timeout='30s')
    hits = res['hits']['hits']
    scroll_id = res.get('_scroll_id')
    process_one_round(es, db, hits, not_found_list)

    stop = False
    while not stop:
        res = es.scroll(scroll_id=scroll_id, scroll='5m')
        hits = res['hits']['hits']
        scroll_id = res.get('_scroll_id')

        if len(hits) == 0:
            stop = True
            continue
        
        process_one_round(es, db, hits, not_found_list)
        # stop = True

    df = pd.DataFrame(not_found_list)

    df.to_csv('../../docs/20220203_docs_without_json.csv', index=False)
    


if __name__ == '__main__':
    credentials = '../../conf/local/credentials.yaml'
    es = get_elasticsearch_conn(credentials)
    db = get_db_conn(credentials)

    update_index(es, db, query_size=5)


            

            








            



