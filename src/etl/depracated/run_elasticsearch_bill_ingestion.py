import os
import sys
project_path = '.'
sys.path.append(project_path)

import logging

from elasticsearch import Elasticsearch
from datetime import datetime

from src.utils.general import get_s3_credentials
from src.etl.ingest_legiscan_dump_es import insert_legiscan_bills_es


timestr = datetime.now().strftime("%y%m%d%H%M%S")
logging.basicConfig(level=logging.INFO, filename=f"logs/legiscan_dump_es_ingestion_{timestr}.DEBUG", filemode='w')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

creds_folder = 'conf/local/'
fpath = os.path.join(creds_folder, 'credentials.yaml')

def run_es_ingestion():
    all_states = [
        'AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 
        'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 
        'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 
        'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 
        'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 
        'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
    s3_creds = get_s3_credentials(fpath)
    es = Elasticsearch([{'host':'localhost','port':9200}])

    indexes = indexes={'meta_data': 'bill_meta', 'text': 'bill_text'}
    bash_script = os.path.join(project_path, 'infrastructure', 'backup_es_indices.sh')

    data_dump_folder = 'legiscan_dump_20200615'
    s3_bills_folder = 'extracted_bill_docs_parallelized'
    exclude_states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DC', 'DE', 
        'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 
        'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 
        'MS', 'MT', 'NC']
    insert_legiscan_bills_es(
        es_conn=es, 
        s3_creds=s3_creds, 
        data_dump_folder=data_dump_folder,
        s3_bills_folder=s3_bills_folder, 
        indexes=indexes,
        backup_ind_shell_script=bash_script,
        exclude_states=exclude_states
    )


if __name__ == '__main__':
    run_es_ingestion()
