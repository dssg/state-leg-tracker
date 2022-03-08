import logging
import os
import json
from src.utils.general import get_elasticsearch_conn
from src.etl.ingest_legiscan_dump_es import _get_subdir
import base64

es = get_elasticsearch_conn('../../conf/local/credentials.yaml')
    # dump_location = '/mnt/data/projects/aclu_leg_tracker/legiscan_dump_20200615_processed'
dump_location = '/mnt/data/projects/aclu_leg_tracker/legiscan_dump_20210709_processed'

LEGISCAN_PLACEHOLDER_DATE = '0000-00-00'
DEFAULT_DATE = '1970-01-01'

def store_bill_texts(es_conn, text_files, folder_path, index_name):
    """
        Given a set of bill text files, store them in an elasticsearch index.
        Here the unit of analysis is a bill text version (document). 
        One bill can have multiple versions. Each version has its on id (doc_id).
        The texts are contained in the dump in PDF format and should be decoded before ingestion.

        Args:
            es_conn: Elasticsearch connection object
            text_files (List[str]): List of file names containing bill doc info (json files)
            folder_path (str): Location where the text files are located
            index_name (str): The name of the elasticsearch index to store the bill_texts 
    """
    # Joining the folder and the file name to create the path
    file_paths = [ os.path.join(folder_path, x) for x in text_files if x.endswith('.json')]

    for fp in file_paths:
        logging.info('Processing {}'.format(fp))
        with open(fp) as json_fp:
            # print(json.load(json_fp))
            # doc = json.load(json_fp).get("text")
            content = json.load(json_fp)
            
            doc = content["text"]

            if (doc.get('date') == LEGISCAN_PLACEHOLDER_DATE) or (doc.get('date') is None or (doc.get('date')=='0000-00-00')):
                doc['date'] = DEFAULT_DATE
                
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
                "url": doc.get('url'),
                "latest_version": False,
                "still_active": False,
                "passage_prediction": "Likely",
                "passage_prediction_date":  doc['date']
            }

            # The document is indexed by the <bill_id>_<doc_id>
            try:
                es_conn.index(
                    index=index_name, 
                    id='{}_{}'.format(doc['bill_id'], doc['doc_id']), 
                    body=json.dumps(json_object)
                )
            except ConnectionRefusedError:
                logging.error('Connection error. Aborting!')
                raise ConnectionRefusedError('Connection refused by elasticsearch instance')
            except ConnectionError:
                logging.error('Connection error. Aborting!')
                raise ConnectionError('Connection refused by elasticsearch instance')
            except Exception as e:
                logging.warning('Could not index {} due to exception {}'.format(doc['doc_id'], e) )
                # raise ValueError('Aborting!')



def update_index(es, dump_location):
    states = _get_subdir(dump_location)  

    # random.shuffle(states)
    logging.info('Processing states in this order: {}'.format(states))

    state_counter = 0
    session_counter = 0
    text_counter = 0

    for state in states:
        logging
        if state in {'US', 'AK', 'AL'}:
            continue

        tpth = os.path.join(dump_location, state)

        # Each session has a sub directory
        sessions = _get_subdir(tpth)

        for session in sessions:
            logging.info('Processing state {}, session {}'.format(state, session))
            tpth2 = os.path.join(tpth, session)

            text_path = os.path.join(tpth2, 'text')
            text_files = os.listdir(text_path) if os.path.isdir(text_path) else []  
            store_bill_texts(es_conn=es, text_files=text_files, folder_path=text_path, index_name='cloned_bill_text_20211119')


if __name__ == '__main__':
    update_index(
        es, dump_location
    )