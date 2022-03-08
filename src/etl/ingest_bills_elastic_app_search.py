import os
import logging
import json

import src.utils.project_constants as constants

from src.utils.general import get_boto3_session, get_elastic_app_search_client
from src.etl.ingest_legiscan_dump_es import _extract_state_session_list

creds_folder = '../../conf/local/'
creds_file = os.path.join(creds_folder, 'credentials.yaml')

s3_session = get_boto3_session(creds_file)
s3 = s3_session.resource('s3')

s3_bucket = constants.S3_BUCKET

s3_bills_folder = 'extracted_bill_docs_parallelized'
data_dump_folder = 'legiscan_dump_20200615'


def _extract_the_elements_to_index(bill_json):
    temp = dict()

    use_keys = [
        'bill_id',
        'session_id',
        'url',
        'state_link',
        'bill_type',
        'bill_number',
        'state',
        'title',
        'description'
    ]

    temp = {x: bill_json[x] for x in use_keys}

    return temp

state_sessions = _extract_state_session_list(s3_session, data_dump_folder)

# one session from each sate
state_sessions_temp = {
    k: [v[0]] for k, v in state_sessions.items()
}

documents_to_index = list()
for state, sessions in state_sessions.items():

    print('Processing state: {}'.format(state))

    for sess in sessions:
        s3_prefix_filter = f'{s3_bills_folder}/{sess}_'
        bill_files = s3.Bucket(s3_bucket).objects.filter(Prefix=s3_prefix_filter)

        for i, bill in enumerate(bill_files):
            json_txt = bill.get()['Body'].read()
            bill_json = json.loads(json_txt)

            uploaded_bill = _extract_the_elements_to_index(bill_json)
            
            documents_to_index.append(uploaded_bill)

            if i > 10:
                break


engine_name = 'bills-search-test'
client = get_elastic_app_search_client(creds_file)

print('indexing {} docs'.format(len(documents_to_index)))

for doc in documents_to_index:
    client.index_document(engine_name, doc)

# client.create_engine(
#     engine_name='test-engine'
# )