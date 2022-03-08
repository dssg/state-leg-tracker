import os
import sys
import logging
import json
import boto3
import subprocess
import random
import multiprocessing as mp

from elasticsearch import Elasticsearch
from datetime import datetime

import src.utils.project_constants as constants
from src.utils.general import get_s3_credentials, get_elasticsearch_conn

logging.basicConfig(level=logging.INFO, filename=f"../../logs/legiscan_dump_es_ingestion_2021_dump.DEBUG", filemode='w')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

creds_folder = '../../conf/local/'
credentials_file = os.path.join(creds_folder, 'credentials.yaml')

"""
    Before reindexing clone the indexes using the following commands in elasticsearch

    1. add write protection to the index

    PUT <index_name>/_settings
    {
        "index": {
        blocks.write": true
    }

    2. create the clone

    POST /<index_name>/_clone/<index_name>_cloned_<date>

    3. reverse the write protection

    PUT <index_name>/_settings
    {
        "index": {
        blocks.write": false
    }

""" 

# The default data we use for any field with a missing date
# The missing dates need to be handled appropriately for downstream tasks
DEFAULT_DATE = '1970-01-01'

# The place holder used by legiscan for a missing date field. 
# We can't use this in elasticsearch. THerefore, we replace this with the DEFAULT DATE GIVEN above
LEGISCAN_PLACEHOLDER_DATE = '0000-00-00'

def _get_subdir(dir_path):
    """In a given directory, return all the immediate subdirectories"""

    subdir = [
        x for x in os.listdir(dir_path) 
        if os.path.isdir(os.path.join(dir_path, x))
    ]

    return subdir


def _distribute_jobs(num_files, n_jobs):
    """Given a number of tasks divide it across processes"""

    # Parallelizing the decoding process
    if (n_jobs > mp.cpu_count()) or n_jobs==-1:
        n_jobs = mp.cpu_count()

    chunk_size, mod = divmod(num_files, n_jobs)
    chunks_list = [chunk_size] * n_jobs

    # Distributing the remainder
    for i in range(mod):
        chunks_list[i] = chunks_list[i] + 1
    
    return chunks_list



def ingest_dump_es(
    es_conn, 
    processed_data_dump_path, 
    n_jobs=-1
    ):
    """
    Insert the data from the legiscan dump to elasticsearch indexes:

    args:
        es_conn: elasticsearch connection object
        processed_data_dump_path (str): The folder where the dump is located. 
            The zip archives in the data dump need to be unzipped and the documents ned to be decoded before running this script.
            Has to be a FS path. Doesn't handle S3 buckets, yet.  
        n_jobs: Number of processes to distribute the jobs across. If -1, all available cores are used   
    """
    states = _get_subdir(processed_data_dump_path)  

    # random.shuffle(states)
    logging.info('Processing states in this order: {}'.format(states))

    state_counter = 0
    session_counter = 0
    bill_counter = 0
    text_counter = 0
    people_counter = 0 # not unique people, but unique person, session pairs
    
    logging.info('Starting ingestion')
    for state in states:
        tpth = os.path.join(processed_data_dump_path, state)

        # Each session has a sub directory
        sessions = _get_subdir(tpth)

        for session in sessions:
            logging.info('Processing state {}, session {}'.format(state, session))
            tpth2 = os.path.join(tpth, session)

            # Bills
            bills_path = os.path.join(tpth2, 'bill')
            bills_files = os.listdir(bills_path) if os.path.isdir(bills_path) else []

            logging.info('Contains {} bills'.format(len(bills_files)))
            
            if n_jobs==1:
                store_bills(es_conn=es_conn, bill_files=bills_files, folder_path=bills_path, index_name='bill_meta')
                logging.info('Processing sequentially')
            else:    
                job_chunks = _distribute_jobs(len(bills_files), n_jobs)

                logging.info('Distributing the bills across {} cores'.format(len(job_chunks)))

                idx_cursor = 0
                jobs = []
                for i, chunk_size in enumerate(job_chunks):
                    p = mp.Process(
                        name=f'p{i}',
                        target=store_bills,
                        kwargs={
                            'es_conn': es_conn,
                            'bill_files': bills_files[idx_cursor: idx_cursor+chunk_size],
                            'folder_path': bills_path,
                            'index_name': 'bill_meta'
                        }
                    )

                    jobs.append(p)
                    p.start()

                    idx_cursor = idx_cursor + chunk_size

                for proc in jobs:
                    proc.join()

            bill_counter = bill_counter + len(bills_files)
            logging.info('Completed bills. Now on to bill texts')
            
            # Text
            text_path = os.path.join(tpth2, 'text')
            text_files = os.listdir(text_path) if os.path.isdir(text_path) else []    

            logging.info('Contains {} bill text versions'.format(len(text_files)))
            if n_jobs==1:
                store_bill_texts(es_conn=es_conn, text_files=text_files, folder_path=text_path, index_name='bill_text')
                logging.info('Processing sequentially')
            else:
                job_chunks = _distribute_jobs(len(text_files), n_jobs)

                logging.info('Distributing the texts across {} cores'.format(len(job_chunks)))

                idx_cursor = 0
                jobs = []
                for i, chunk_size in enumerate(job_chunks):
                    p = mp.Process(
                        name=f'p{i}',
                        target=store_bill_texts,
                        kwargs={
                            'es_conn': es_conn,
                            'text_files': text_files[idx_cursor: idx_cursor+chunk_size],
                            'folder_path': text_path,
                            'index_name': 'bill_text'
                        }
                    )

                    jobs.append(p)
                    p.start()

                    idx_cursor = idx_cursor + chunk_size
                
                for proc in jobs:
                    proc.join()

            text_counter = text_counter + len(text_files)
            logging.info('Completed texts. Now on to people in the session')

            # people
            # We need the session_id to append to the people json file
            # We can find it in a bill json
            b = os.path.join(bills_path, bills_files[0])
            with open(b) as bfp:
                session_id = json.load(bfp)['bill']['session_id']


            people_path = os.path.join(tpth2, 'people')
            people_files = os.listdir(people_path) if os.path.isdir(people_path) else []  
            job_chunks = _distribute_jobs(len(people_files), n_jobs)

            logging.info('Session has {} people'.format(len(people_files)))
            # logging.info('Distributing them across {} cores'.format(len(job_chunks)))
            logging.info('Processing sequentially')

            store_people(
                es_conn=es_conn,
                people_files=people_files,
                folder_path=people_path,
                session_id=session_id,
                index_name='session_people'
            )

            people_counter = people_counter + len(people_files)
            session_counter = session_counter + 1
        
        state_counter = state_counter + 1

    logging.info('Completed the ingestion! There were {} bills, {} texts, and {} people files in the dump across {} states and {} sessions'.format(
        bill_counter, text_counter, people_counter, state_counter, session_counter
    ))


def store_bills(es_conn, bill_files, folder_path, index_name):
    """
        Given a set of bill files, store them in elasticsearch
        Args:
            es_conn: Elasticsearch connection object
            bill_files (List[str]): List of file names containing bill info (json files)
            folder_path (str): Location where the bill files are located
            index_name (str): The name of the elasticsearch index to store the bills 
    """

    # Joining the folder and the file name to create the path
    file_paths = [ os.path.join(folder_path, x) for x in bill_files]

    for fp in file_paths:
        with open(fp) as json_fp:
            bill_json = json.load(json_fp)['bill']

        if bill_json.get('status_date') == LEGISCAN_PLACEHOLDER_DATE:
            bill_json['status_date'] = DEFAULT_DATE

        # Fields which has date in their sub elements
        fields_with_date = ['progress', 'history', 'votes', 'amendments', 'calendar', 'texts']
        for field in fields_with_date:
            for item in bill_json.get(field):
                if item.get('date') == LEGISCAN_PLACEHOLDER_DATE:
                    item['date'] = DEFAULT_DATE

        # Explicitly defining the json structure for future change/debugging purposes
        json_object = {
            "bill_id": bill_json.get('bill_id'),
            "bill_number": bill_json.get('bill_number'),
            "bill_type":  bill_json.get('bill_type'),
            "bill_type_id": bill_json.get('bill_type_id'),
            "body": bill_json.get('body'),
            "body_id": bill_json.get('body_id'),
            "change_hash": bill_json.get('change_hash'),
            "committee": bill_json.get('committee'),
            "pending_committee_id": bill_json.get('pending_committee_id'),
            "current_body": bill_json.get('current_body'),
            "current_body_id": bill_json.get('current_body_id'),
            "description": bill_json.get('description'),
            "history": bill_json.get('history'),
            "session": bill_json.get('session'),
            "session_id": bill_json.get('session_id'),
            "sponsors": bill_json.get('sponsors'),
            "sats": bill_json.get('sats'),
            "state": bill_json.get('state'),
            "state_id": bill_json.get('state_id'),
            "state_link": bill_json.get('state_link'),
            "status": bill_json.get('status'),
            "status_date": bill_json.get('status_date'),
            "subjects": bill_json.get('subjects'),
            "title": bill_json.get('title'),
            "url": bill_json.get('url'),
            "votes": bill_json.get('votes'),
            "completed": bill_json.get('completed'),
            "amendments": bill_json.get('amendments'),
            "calendar": bill_json.get('calendar'),
            "progress": bill_json.get('progress'),
            "texts": bill_json.get('texts')
        }

        # Indexing the document. 
        # The doc id in elasticsearch is the bill_id
        # If the document already exists, it will replace it
        try: 
            es_conn.index(
                index=index_name, 
                id=bill_json['bill_id'], 
                body=json.dumps(json_object)
            )
        except ConnectionRefusedError:
            logging.error('Connection error. Aborting!')
            raise ConnectionRefusedError('Connection refused by elasticsearch instance')
        except ConnectionError:
            logging.error('Connection error. Aborting!')
            raise ConnectionError('Connection refused by elasticsearch instance')
        except Exception as e:
            logging.warning('Could not index {} due to exception {}'.format(bill_json['bill_id'], e) )
            raise ValueError('Aborting!')



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
                
            # TODO: The bill json file contains a `texts` field. We can use that to impute the missing doc dates.  
            json_object = {
                "bill_id": doc['bill_id'],
                "doc_date": doc['date'],
                "doc": doc.get('doc_decoded'),
                # "encoded_doc": doc.get('doc'),
                "doc_id": doc['doc_id'],
                "mime": doc['mime'],
                "mime_id": doc['mime_id'],
                "state_link": doc.get('state_link'),
                "text_size": doc['text_size'],
                "type": doc['type'],
                "type_id": doc['type_id'],
                "url": doc.get('url')
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
                raise ValueError('Aborting!')


def store_people(es_conn, people_files, folder_path, index_name, session_id):
    """
    Given a list of people files, index them in elasticsearch
    Args:
        es_conn: Elasticsearch connection object
        people_files (List[str]): List of file names containing people info (json files)
        folder_path (str): Location where the text files are located
        index_name (str): The name of the elasticsearch index to store the bill_texts
    """

    # Joining the folder and the file name to create the path
    file_paths = [ os.path.join(folder_path, x) for x in people_files]

    for fp in file_paths:
        # We add the session_id to the document content and the document id
        with open(fp) as json_fp:
            person_json = json.load(json_fp)['person']
        
        json_object = {
            'session_id': session_id,
            'people_id': person_json.get('people_id'),
            'person_hash': person_json.get('person_hash'),
            'state_id': person_json.get('state_id'),
            'party_id': person_json.get('party_id'),
            'party': person_json.get('party'),
            'role_id': person_json.get('role_id'),
            'role': person_json.get('role'),
            'name': person_json.get('name'),
            'first_name': person_json.get('first_name'),
            'middle_name': person_json.get('middle_name'),
            'last_name': person_json.get('last_name'),
            'suffix': person_json.get('suffix'),
            'nickname': person_json.get('nickname'),
            'district': person_json.get('district'),
            'ftm_eid': person_json.get('ftm_eid'),
            'votesmart_id': person_json.get('votesmart_id'),
            'opensecrets_id': person_json.get('opensecrets_id'),
            'ballotpedia': person_json.get('ballotpedia'),
            'committee_sponsor': person_json.get('committee_sponsor'),
            'committee_id': person_json.get('committee_id')
        }

        # The document is indexed by the <session_id>_<people_id>
        try: 
            es_conn.index(
                index=index_name, 
                id='{}_{}'.format(session_id, person_json.get('people_id')), 
                body=json.dumps(json_object)
            )
        except ConnectionRefusedError:
            logging.error('Connection error. Aborting!')
            raise ConnectionRefusedError('Connection refused by elasticsearch instance')
        except ConnectionError:
            logging.error('Connection error. Aborting!')
            raise ConnectionError('Connection refused by elasticsearch instance')
        except Exception as e:
            logging.warning('Could not index {} due to exception {}'.format(person_json.get('people_id'), e) )
            raise ValueError('Aborting!')



def run():
    """ Run the elasticsearch ingestion for bills and people"""

    es = get_elasticsearch_conn(credentials_file)
    # dump_location = '/mnt/data/projects/aclu_leg_tracker/legiscan_dump_20200615_processed'
    dump_location = '/mnt/data/projects/aclu_leg_tracker/legiscan_dump_20210709_processed'

    # Multiple processes writing to Elasticsearch seems to throw errors, so keep n_jobs=1 for now
    ingest_dump_es(
        es_conn=es,
        processed_data_dump_path=dump_location,
        n_jobs=1
    )

if __name__ == '__main__':
    run()




## Old functions
# TODO -- Remove after confirming the new code's data integrity
# def _extract_state_session_list(s3_session, data_dump_folder):
#     """Organize the datasets into their sessions and states so the upload can be systematic and backups can be done. 
#         Returns a dictionary with the state as a key and list of session number as the value

#         Assumption: Zip files take the form <state_abbrev>_<session_name>_[<session_num>].zip 
#     """

#     s3 = s3_session.resource('s3')
#     s3_bucket = constants.S3_BUCKET

#     datasets = s3.Bucket(s3_bucket).objects.filter(Prefix=data_dump_folder)

#     # Getting the session list for all states from the zip file names
#     state_sessions = dict() # Dict[state, sessions (List)]

#     for f in datasets:
#         if f.key.endswith('zip'):
#             # removing the folder name from the key
#             ds_name = f.key.split('/')[1]

#             # extracting the state name
#             state_name = ds_name.split('_')[0]

#             # Removing the enclosing brackets for the session num
#             session_num = ds_name.split('_')[-1]
#             session_num = session_num.split('.')[0].strip('[')
#             session_num = session_num.strip(']')

#             # Inserting the session number to the dictionary
#             if state_name in state_sessions:
#                 state_sessions[state_name].append(session_num)
#             else:
#                 state_sessions[state_name] = [session_num]

#     return state_sessions


# def insert_legiscan_bills_es(
#     es_conn, 
#     s3_session, 
#     data_dump_folder, 
#     s3_bills_folder, 
#     indexes={'meta_data': 'bill_meta', 'text': 'bill_text'}, 
#     backup_ind_shell_script=None,
#     exclude_states=None
#     ):

#     """ stores all the bills in a  folder in elastic search 
#         Args:
#             es_conn: Elasticsearch connection object
#             s3_session: Boto3 s3 session
#             data_dump_folder: Folder where the zip files in the data dump are stored
#             s3_bills_folder: Folder where the bill json files are stored
#             indexes: The names of the indexes used to store the metadata of the bill and bill texts. The keys should be ['meta_data', 'text'] respectively
#             backup_ind_shell_script: the shell script file to periodically backup the stored indices to disk (optional)
#             exclude_states: A list of states to be excluded from ingestion (optional). 
#                             This is useful when the insertion process was stopped in the middle and is starting back up. 
#                             Saves the time of checking the existence of each bill one by one.  
#     """

#     if backup_ind_shell_script is None:
#         logging.warning('There is no script provided for backing up bills.')
    
#     s3 = s3_session.resource('s3')
#     s3_bucket = constants.S3_BUCKET

#     logging.info('Organizing the sessions into states')
#     state_sessions = _extract_state_session_list(s3_session, data_dump_folder)
    
#     try: 
#         res_meta = es_conn.count(index=indexes['meta_data'])
#         res_text = es_conn.count(index=indexes['text'])
#         logging.info('Elasticsearch contains metadata for {} bills, and texts for {} bill documents. Starting insertion'.format(res_meta['count'], res_text['count']))
#     except:
#         logging.warning('No documents in the indexes. Starting insertion')

#     # The bill storing happens in the order of states and their sessions
#     for state, sessions in state_sessions.items():
#         if (exclude_states is not None) and (state in exclude_states):
#             logging.info('Skipping over {}'.format(state))
#             continue

#         logging.info('Storing state: {}'.format(state))

#         for sess in sessions:
#             # Filter the bills of the respective session
#             logging.info('Storing session: {}'.format(sess))
#             s3_prefix_filter = f'{s3_bills_folder}/{sess}_'
#             bill_files = s3.Bucket(s3_bucket).objects.filter(Prefix=s3_prefix_filter)

#             for bill in bill_files:
#                 json_txt = bill.get()['Body'].read()
#                 bill_json = json.loads(json_txt)

#                 logging.info('Ingesting bill_id: {}'.format(bill_json.get('bill_id')))

#                 # bill metadata
#                 store_bill_meta(es_conn, bill_json, indexes['meta_data'])

#                 # bill text
#                 store_bill_text(es_conn, bill_json, indexes['text'])

#         if backup_ind_shell_script is not None:
#             logging.info('backing up...')
#             with open(backup_ind_shell_script, 'rb') as f:
#                 script = f.read()

#             rc = subprocess.call(script, shell=True)

#     try: 
#         res_meta = es_conn.count(index=indexes['meta_data'])
#         res_text = es_conn.count(index=indexes['text'])
#         logging.info('Completed insertion. Elasticsearch contains metadata for {} bills, and texts for {} bill documents'.format(res_meta['count'], res_text['count']))
#     except:
#         logging.warning('No documents were inserted')
    

# def store_bill_meta(es_conn, bill_json, index):
#     """insert bill metadata to the index for bill meta data"""

#     document_id = bill_json['bill_id']
#     r = es_conn.exists(index, id=document_id)

#     if not r:
#         # adding the date checks
#         default_date = '2001-01-01'
#         placeholder = '0000-00-00'
        
#         if bill_json.get('status_date') == placeholder:
#             bill_json['status_date'] = default_date

#         # Fields which has date in their sub elements
#         fields_with_date = ['progress', 'history', 'votes', 'amendments', 'calendar']
#         for field in fields_with_date:
#             for item in bill_json.get(field):
#                 if item.get('date') == placeholder:
#                     item['date'] = default_date

#         # Explicitly defining for future change/debugging purposes
#         json_object = {
#             "bill_id": bill_json.get('bill_id'),
#             "bill_number": bill_json.get('bill_number'),
#             "bill_type":  bill_json.get('bill_type'),
#             "bill_type_id": bill_json.get('bill_type_id'),
#             "body": bill_json.get('body'),
#             "body_id": bill_json.get('body_id'),
#             "change_hash": bill_json.get('change_hash'),
#             "committee": bill_json.get('committee'),
#             "pending_committee_id": bill_json.get('pending_committee_id'),
#             "current_body": bill_json.get('current_body'),
#             "current_body_id": bill_json.get('current_body_id'),
#             "description": bill_json.get('description'),
#             "history": bill_json.get('history'),
#             "session": bill_json.get('session'),
#             "session_id": bill_json.get('session_id'),
#             "sponsors": bill_json.get('sponsors'),
#             "sats": bill_json.get('sats'),
#             "state": bill_json.get('state'),
#             "state_id": bill_json.get('state_id'),
#             "state_link": bill_json.get('state_link'),
#             "status": bill_json.get('status'),
#             "status_date": bill_json.get('status_date'),
#             "subjects": bill_json.get('subjects'),
#             "title": bill_json.get('title'),
#             "url": bill_json.get('url'),
#             "votes": bill_json.get('votes'),
#             "completed": bill_json.get('completed'),
#             "amendments": bill_json.get('amendments'),
#             "calendar": bill_json.get('calendar'),
#             "progress": bill_json.get('progress')
#         }

    
#         es_conn.index(
#             index=index, 
#             id=document_id, body=json.dumps(json_object)
#         )
#     else:
#         logging.info('Already exists')


# def store_bill_text(es_conn, bill_json, index):
#     """insert bill documents (texts) to the index for bill texts"""

#     bill_docs = bill_json["texts"]
#     desc = bill_json["description"]
#     title = bill_json["title"]

#     for doc in bill_docs:
#         # the document id is the combination of the bill_id and the doc_id
#         document_id = '{}_{}'.format(doc.get('bill_id'), doc.get('doc_id'))
#         r = es_conn.exists(index, id=document_id)

#         if not r:
#             # Setting default value
#             if doc.get('date') == '0000-00-00':
#                 print(f"updating {doc.get('date')}")
#                 doc['date'] = '2000-01-01'
#                 print(f"updating {doc.get('date')}")

#             json_object = {
#                 "bill_id": doc.get('bill_id'),
#                 "doc_date": doc.get('date'),
#                 "doc": doc.get('doc'),
#                 "doc_id": doc.get('doc_id'),
#                 "mime": doc.get('mime'),
#                 "mime_id": doc.get('mime_id'),
#                 "state_link": doc.get('state_link'),
#                 "text_size": doc.get('text_size'),
#                 "type": doc.get('type'),
#                 "type_id": doc.get('type_id'),
#                 "url": doc.get('url'),
#                 "description": desc,
#                 "title": title
#             }

#             es_conn.index(
#                 index=index, 
#                 id=document_id, body=json.dumps(json_object)
#             )
#         else:
#             logging.info('Already exists')


# def store_person(es_conn, person_json, session_id, index):
#     """ Insert person details to an elastic search index """

#     person_idx = '{}_{}'.format(session_id, person_json['people_id'])
#     r = es_conn.exists(index, id=person_idx)

#     if not r:
#         json_object = {
#             'session_id': session_id,
#             'people_id': person_json.get('people_id'),
#             'person_hash': person_json.get('person_hash'),
#             'state_id': person_json.get('state_id'),
#             'party_id': person_json.get('party_id'),
#             'party': person_json.get('party'),
#             'role_id': person_json.get('role_id'),
#             'role': person_json.get('role'),
#             'name': person_json.get('name'),
#             'first_name': person_json.get('first_name'),
#             'middle_name': person_json.get('middle_name'),
#             'last_name': person_json.get('last_name'),
#             'suffix': person_json.get('suffix'),
#             'nickname': person_json.get('nickname'),
#             'district': person_json.get('district'),
#             'ftm_eid': person_json.get('ftm_eid'),
#             'votesmart_id': person_json.get('votesmart_id'),
#             'opensecrets_id': person_json.get('opensecrets_id'),
#             'ballotpedia': person_json.get('ballotpedia'),
#             'committee_sponsor': person_json.get('committee_sponsor'),
#             'committee_id': person_json.get('committee_id')
#         }

#         es_conn.index(
#             index=index, 
#             id=person_idx, 
#             body=json.dumps(json_object)
#         )
#     else:
#         logging.info('Person {} already exists'.format(person_json['people_id']))


# def insert_session_people_es(
#     es_conn, 
#     s3_session, 
#     s3_data_dump,
#     s3_source, 
#     index='session_people',
#     exclude_states=None):

#     """
#         Stores the people in sessions in elasticsearch
#         Args:
#             es_conn: Elasticsearch connection object
#             s3_session: Boto3 session
#             s3_data_dump: Folder where the legiscan dump is stored
#             s3_source: The S3 folder where the extracted json files are stored
#             index: The name of the elastic search index
#     """ 

#     s3 = s3_session.resource('s3')
#     s3_bucket = constants.S3_BUCKET

#     logging.info('Organizing the sessions into states')
#     state_sessions = _extract_state_session_list(
#         s3_session,
#         s3_data_dump
#     )

#     try:
#         res = es_conn.count(index=index)
#         logging.info('Elasticsearch contains {} people'.format(res['count']))
#     except:
#         logging.info('No documents in the index. Starting insertion')

#     # Storing happens in the order of states and their sessions
#     for state, sessions in state_sessions.items():
#         if (exclude_states is not None) and (state in exclude_states):
#             logging.info('Skipping over {}'.format(state))
#             continue

#         logging.info('Storing state: {}'.format(state))

#         for sess in sessions:
#             # People of the sessions
#             logging.info('Storing session: {}'.format(sess))

#             s3_prefix_filter = '{}/{}_'.format(s3_source, sess)

#             people_files = s3.Bucket(s3_bucket).objects.filter(
#                 Prefix=s3_prefix_filter
#             )

#             for person in people_files:
#                 json_txt = person.get()['Body'].read()
#                 person_json = json.loads(json_txt)

#                 logging.info('Ingesting people id: {}'.format(
#                     person_json.get('people_id')
#                 ))

#                 store_person(
#                     es_conn=es_conn,
#                     person_json=person_json,
#                     session_id=sess,
#                     index=index
#                 )


# def run_people_info_ingestion():
#     """ Run the ingestion script for session people information """
#     s3_creds = get_s3_credentials(fpath)
#     session = boto3.Session(
#         aws_access_key_id=s3_creds['aws_access_key_id'],
#         aws_secret_access_key=s3_creds['aws_secret_access_key']
#     )

#     es = get_elasticsearch_conn(fpath)
    
#     data_dump_folder = 'legiscan_dump_20200615'
#     extracted_json_files = 'extracted_session_people'

#     insert_session_people_es(
#         es_conn=es,
#         s3_session=session,
#         s3_data_dump=data_dump_folder,
#         s3_source=extracted_json_files,
#         index='session_people',
#         exclude_states=None
#     )    


# def run_bill_info_ingestion():
#     """ Run the ingestion script for storing bill information in elasticsearch """

#     s3_creds = get_s3_credentials(fpath)
#     session = boto3.Session(
#         aws_access_key_id=s3_creds['aws_access_key_id'],
#         aws_secret_access_key=s3_creds['aws_secret_access_key']
#     )

#     es = get_elasticsearch_conn(fpath)
    
#     data_dump_folder = 'legiscan_dump_20200615'
#     s3_bills_folder = 'extracted_bill_docs_parallelized'
#     indexes = indexes={'meta_data': 'bill_meta', 'text': 'bill_text'}
#     bash_script = os.path.join('../../', 'infrastructure', 'backup_es_indices.sh')


#     insert_legiscan_bills_es(
#         es_conn=es,
#         s3_session=session,
#         data_dump_folder=data_dump_folder,
#         s3_bills_folder=s3_bills_folder,
#         indexes=indexes,
#         backup_ind_shell_script=bash_script,
#         exclude_states=None
#     )


# def main():
#     """ Run the elasticsearch ingestion for bills and people"""

#     # The type of data to ingest as a user input
#     try:
#         data_type = sys.argv[1]

#         if data_type not in ['people', 'bill']:
#             raise ValueError('The data type should be one of (people, bill)')
#     except:
#         raise ValueError('Should provide a data type to in insert from (people, bill)')

#     if data_type == 'people':
#         run_people_info_ingestion()
#     else:
#         run_bill_info_ingestion()


# if __name__ == '__main__':
#     run()