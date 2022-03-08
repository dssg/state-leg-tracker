import os
import sys
import logging

from datetime import datetime
from elasticsearch import Elasticsearch
from typing import Dict

from src.pipeline.label_data_es import get_bills_in_issue_area
from src.utils.general import get_db_conn


timestr = datetime.now().strftime("%y%m%d%H%M%S")
logging.basicConfig(level=logging.INFO, filename=f"../../logs/label_data_es_{timestr}.DEBUG", filemode='w')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

creds_folder = '../../conf/local/'
creds_file = os.path.join(creds_folder, 'credentials.yaml')


def label_data_es(issue_area_phrases: Dict, data_folder: str):
    es = Elasticsearch([{'host':'localhost','port':9200}])

    for issue, kw in issue_area_phrases.items():
        logging.info('Labeling {}'.format(issue))

        df = get_bills_in_issue_area(
            es=es,
            search_index='bill_text',
            search_key='doc',
            search_phrases = kw,
            issue_area=issue,
            query_size=10
        )

        save_path = os.path.join(data_folder, '{}_labels.csv'.format(issue))
        logging.info('Saving labels for {} issue area using {} keywords at {}'.format(issue, kw,save_path))
        df.to_csv(save_path, index=False)

def upload_data_to_db(data_folder):
    db_con = get_db_conn(creds_file)

    label_files = {x.rsplit('_', 1)[0]:x for x in os.listdir(data_folder) if x.endswith('.csv')}
    
    for table, fname in label_files.items():
        cur = db_con.cursor()
        csv_path = os.path.join(data_folder, fname)
        with open(csv_path, 'r') as f:
            next(f)
            cur.copy_from(f, f'labels_es.{table}', sep=',')
        db_con.commit()


if __name__ == '__main__':
    data_path = '../../data/labels_es/'
    issue_search_phrases = {
        'immigrant_rights': ["immigrants' rights", "immigrant"],
        'voting_rights': ['voter protection', 'mail in vote', 'voting rights'],
        'criminal_law_reform': ['criminal law', 'criminal justice', 'policing', 'police brutality'],
        'racial_justice': ['racial justice', 'segregation', 'police brutality', 'housing'],
        'lgbt_rights': ['lgbtq', 'lesbian', 'gay', 'bisexual', 'transgender'],
        'privacy_technology': ['digital footprint', 'data privacy']
    }

    label_data_es(issue_search_phrases, data_path)
    upload_data_to_db(data_path)
