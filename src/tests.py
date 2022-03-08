import os
import sys
project_path = '.'
sys.path.append(project_path)

import pandas as pd
import logging
import boto3
import base64
from zipfile import ZipFile
import tempfile
from urllib.request import urlopen
import requests
# -rom lxml import html
from bs4 import BeautifulSoup
import re
import json
import time
import matplotlib.pyplot as plt

from elasticsearch import Elasticsearch
from triage.component.timechop import Timechop


# logging.basicConfig(level=logging.DEBUG, filename="log_legiscan_tests.debug", filemode='w')
logging.basicConfig(level=logging.INFO, filename="tests.debug", filemode='w')
# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

from src.utils.general import get_legiscan_key, get_db_conn, get_s3_credentials
from src.utils.decoders import pdf_decoder
from src.etl.legiscan_interface import get_state_sessions, get_bills_in_session, get_bill_info, get_bill_content, get_available_datasets, get_dataset_content
from src.etl.populate_table_functions import populate_sessions, populate_bills, populate_datasets
from src.etl.legiscan_data_loader import fresh_load
from src.etl.parsing_datasets import extract_datasets_in_db, add_bill_text_to_db_content
from src.utils import project_constants as constants
from src.etl.bill_doc_creator import parse_session_zip_file
from src.etl.legiscan_dump_parser import parse_legiscan_dump

# from src.etl.ingest_legiscan_dump_es import insert_legiscan_bills_es

from src.pipeline.label_data_es import get_bills_in_issue_area
from src.pipeline.eda_functions import get_repro_bill_texts

# from src.utils.elasticsearch import fetch_all_docs_bill_text
from src.pipeline.eda_functions import repro_word_cloud_country
from src.utils.project_constants import S3_BUCKET

from src.utils.visualize_timechop import visualize_chops





creds_folder = 'conf/local/'
fpath = os.path.join(creds_folder, 'credentials.yaml')

def test_sessions():
    key = get_legiscan_key(fpath)
    sessions = get_state_sessions(key, state='PA')
    
    ses_id = sessions[0]['session_id']

    bills = get_bills_in_session(key, session_id=ses_id)

    # all bill_ids
    bill_ids = [x.get('bill_id') for x in bills.values()]
    # for bill_id in bill_ids[:5]:
    #     logging.debug('parsing bill_id: {}'.format(bill_id))
    #     if bill_id is not None:
    #         # get_bill_text(key, bill_id=bill_id)
    #         # get_bill_info(key, bill_id=bill_id)

    # get_bill_text(key, bill_id=bill_ids[4])
    get_bill_info(key, bill_id=bill_ids[4])

def test_db_pop():
    key = get_legiscan_key(fpath)
    db_con = get_db_conn(fpath)
    states_list = ['PA']
    # populate_sessions(db_con, key, states_list)

    sessions = get_state_sessions(api_key=key, state=states_list[0])
    sid = sessions[0]['session_id']
    logging.debug(sid)
    populate_bills(db_con, key, [sid])


def test_loader_script():
    key = get_legiscan_key(fpath)
    db_con = get_db_conn(fpath)

    fresh_load(db_con, key)


def test_bill_decoding():
    key = get_legiscan_key(fpath)
    db_con = get_db_conn(fpath)

    sessions = get_state_sessions(key, state='PA')
    ses_id = sessions[0]['session_id']

    bills = get_bills_in_session(key, session_id=ses_id)
    bill_ids = [x.get('bill_id') for x in bills.values()]
    
    bill_docs = get_bill_content(key, bill_ids[1])

    logging.debug(bill_docs)

def test_datasets():
    key = get_legiscan_key(fpath)
    # get_available_datasets(key)
    sess_id = 1706
    access_key = '5b2eyCpx19cB0mfGOYharl'
    get_dataset_content(key, sess_id, access_key)


def test_dataset_upload():
    key = get_legiscan_key(fpath)
    s3_creds = get_s3_credentials(fpath)
    db_con = get_db_conn(fpath)

    populate_datasets(db_con, key, s3_creds)

def test_dataset_decode():
    key = get_legiscan_key(fpath)
    s3_creds = get_s3_credentials(fpath)
    db_con = get_db_conn(fpath)

    s3_bucket ='aclu-leg-tracker'
    # folder = 'raw/{}/{}'.format(ds['session_id'], date_formatted) 
    # s3_path = 's3://{}/{}'.format(s3_bucket, folder)
    # s3_file_key = '{}/198bc880adde4e38b9f8a198e26479e6'.format(folder, )

    # s3_file_key = 'raw/1706/20200607/198bc880adde4e38b9f8a198e26479e6'

    s3_file_key = "raw/100/20200202/39f354468191411bbbd6419c8687c63e"

    # "s3://aclu-leg-tracker"

    session = boto3.Session(
            aws_access_key_id=s3_creds['aws_access_key_id'],
            aws_secret_access_key=s3_creds['aws_secret_access_key']
    )
    s3 = session.resource('s3')
    
    obj = s3.Object(s3_bucket, s3_file_key)
    encoded_text  = obj.get()['Body'].read()

    decoded_text = base64.b64decode(encoded_text)
    logging.info(decoded_text)

def test_download_file():
    
    folder = "./extracted_file"
    states = os.listdir(folder)
    state = states[0]

    sessions = os.listdir(os.path.join(folder, state))
    session= sessions[0]

    bills = os.listdir(os.path.join(folder, state, session, 'bill'))
    bills = bills[:5]

    for bill in bills:
        bill_path = os.path.join(folder, state, session, 'bill', bill)
        with open(bill_path) as f:
            bill_info = json.load(f)

        docs = bill_info.get('bill').get('texts')
        logging.info(bill_info.get('bill_id'))

        for doc in docs:
            url = doc.get('state_link')
            # logging.info('{}: {}'.format(doc.get('doc_id'), url))
            pdf_file = requests.get(url)
            
            # logging.info(pdf_file.content)
            
            decoded = pdf_decoder(pdf_file.content)
            
            logging.info(decoded)
            


    # logging.info(states)
    # logging.info(sessions)
    # logging.info(bills)

    # url = "https://legiscan.com//AL//text//HB95//id//2119294"
    # # url2= "https://legiscan.com/AL/text/HB95/id/2119294/Alabama-2020-HB95-Introduced.pdf"
    # # r = urlopen(url)
    # # # print(r)
    # # logging.info(r)
    # # f = open("test.pdf", 'wb')
    # # f.write(r.read())
    # # f.close()

    # page = requests.get('https://legiscan.com//AL//text//HB95//id//2119294')
    # tree = html.fromstring(page.content)
    # # logging.info(tree)

    # content = tree.xpath('//div[@title="gaits-wrapper"]')

    # soup = BeautifulSoup(page.text)

    # # for link in soup.findAll('a', attrs={'href': re.compile("/AL/text")}):
    # # # for link in soup.findAll('a'):
    # #     logging.info(link)

    # for link in soup.select("a[href$='.pdf']"):
    #     logging.info(link.get('href'))


def test_extract_datasets():
    key = get_legiscan_key(fpath)
    s3_creds = get_s3_credentials(fpath)
    db_con = get_db_conn(fpath)

    extract_datasets_in_db(db_con, s3_creds)

def test_extracting_bill_content():
    key = get_legiscan_key(fpath)
    s3_creds = get_s3_credentials(fpath)
    db_con = get_db_conn(fpath)

    add_bill_text_to_db_content(db_con, s3_creds)

def test_bill_doc_creation():
    key = get_legiscan_key(fpath)
    s3_creds = get_s3_credentials(fpath)
    db_con = get_db_conn(fpath)

    # file_path = "legiscan_dump_20200615/AK_2011-2012_27th_Legislature_[122].zip"
    # file_path = "legiscan_dump_20200615/AL_2011-2011_Organizational_Session_[158].zip"

    # session = boto3.Session(
    #     aws_access_key_id=s3_creds['aws_access_key_id'],
    #     aws_secret_access_key=s3_creds['aws_secret_access_key']
    # )
    # s3 = session.resource('s3')
    # s3_bucket = constants.S3_BUCKET
    # obj = s3.Object(s3_bucket, file_path)
    # zip_content  = obj.get()['Body'].read()

    # # # logging.info(encoded_text)
    # # decoded_text = base64.b64decode(encoded_text)
    # bill_docs = parse_session_zip_file(zip_content)

    # for b in bill_docs:
    #     logging.info(bill_docs[0])
    #     d = b['texts'][0]['doc']
    #     logging.info(d)
        
    #     # decoded_text = base64.b64decode(d, validate=True)
    #     # text = pdf_decoder(decoded_text)

    # # logging.info(text)
    st = time.perf_counter()
    parse_legiscan_dump(s3_creds, 'legiscan_dump_20200615', 'extracted_bill_docs_parallelized', n_jobs=-1)
    en = time.perf_counter()

    logging.info('parallel time {}'.format(en-st))

    # st = time.perf_counter()
    # parse_legiscan_dump(s3_creds, 'legiscan_dump_20200615', n_jobs=1)
    # en = time.perf_counter()

    # logging.info('serial time {}'.format(en-st))

# def main(system_args):
#     creds_fpath = system_args[1]
#     s3_source_folder = system_args[2]
#     s3_target_folder = system_args[3]
#     n_jobs = system_args[4]
#     s3_creds = get_s3_credentials(creds_fpath)

#     st = time.perf_counter()
#     parse_legiscan_dump(s3_creds, s3_source_folder, s3_target_folder, n_jobs)
#     en = time.perf_counter()

#     logging.info('Time elapsed {} seconds'.format(en-st))


# if __name__ == '__main__':
#     main(sys.argv)

def test_es_ingestion():
    s3_creds = get_s3_credentials(fpath)
    es = Elasticsearch([{'host':'localhost','port':9200}])

    s3_bucket = constants.S3_BUCKET
    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )
    s3 = session.resource('s3')

    indexes={'meta_data': 'bill_meta', 'text': 'bill_text'}
    # indexes={'meta_data': 'bill_meta_test', 'text': 'bill_text_test'}

    s3_folder = 'extracted_bill_docs_parallelized'
    insert_legiscan_bills_es(es, s3_creds, s3_folder, indexes)

    search_query = { 
        "query" : { 
            "match_all" : {} 
        },
        "stored_fields": []
    }

    res_meta = es.search(index=indexes['meta_data'], body=search_query)
    res_text = es.search(index=indexes['text'], body=search_query)

    logging.info(res_meta)
    logging.info(res_text)

def test_bill_labeling():
    es = Elasticsearch([{'host':'localhost','port':9200}])

    df = get_bills_in_issue_area(
        es=es,
        search_index='bill_text',
        search_key='doc',
        search_phrases=['abortion', 'reproductive rights'],
        issue_area='reproductive_rights',
        query_size=100
    )

    save_path = os.path.join(project_path, 'data', 'reproductive_rights_labels_all.csv')
    df.to_csv(save_path, index=False)

def upload_bill_labels():
    db_con = get_db_conn(fpath)
    cur = db_con.cursor()
    csv_path = os.path.join(project_path, 'data', 'reproductive_rights_labels_all.csv')

    with open(csv_path, 'r') as f:
        next(f)
        cur.copy_from(f, 'labels_es.reproductive_rights', sep=',')

    db_con.commit()


def upload_results_csv_to_postgres():
    logging.info('Copy results to SQL')
    db_con = get_db_conn(fpath)

    cur = db_con.cursor()
    csv_path = os.path.join(project_path, 'data', 'all_bill_docs.csv')

    with open(csv_path, 'r') as f:
        next(f)
        cur.copy_from(f, 'temp_eda.bill_docs', sep=',')

    db_con.commit()

def fetch_all_bill_docs():
    es = Elasticsearch([{'host':'localhost','port':9200}])
    df = fetch_all_docs_bill_text(
        es=es,
        index='bill_text'
    )

    save_path = os.path.join(project_path, 'data', 'all_bill_docs.csv')
    df.to_csv(save_path, index=False)

def fetch_repro_bill_texts():
    es = Elasticsearch([{'host':'localhost','port':9200}])
    db_con = get_db_conn(fpath)

    bill_texts = get_repro_bill_texts(es, db_con)
    save_path = os.path.join(project_path, 'data', 'repro_bill_texts.csv')
    bill_texts.to_csv(save_path, index=False)


def generate_cloud_country():
    es = Elasticsearch([{'host':'localhost','port':9200}])
    db_con = get_db_conn(fpath)
    figures_path = os.path.join(project_path, 'notebooks', 'images')

    file_path = os.path.join(project_path, 'data', 'repro_bill_texts.csv')
    texts = pd.read_csv(file_path)
    wordcloud_country = repro_word_cloud_country(es, db_con, texts)
    # wordcloud_country = repro_word_cloud_country(es, db_con)


    plt.figure(figsize = (8, 8), facecolor = None) 
    plt.imshow(wordcloud_country) 
    plt.axis("off") 
    plt.tight_layout(pad = 0) 

    fname = 'eda_3_1_country.png'
    plt.tight_layout()
    plt.savefig(os.path.join(figures_path, fname), dpi=300, bbox_inches='tight')


def test_bash_script():
    import subprocess
    # bash_script = os.path.join(project_path, 'infrastructure', 'test_script.sh')
    bash_script = os.path.join(project_path, 'infrastructure', 'backup_es_indices.sh')
    
    # rc = subprocess.call(bash_script)

    with open(bash_script, 'rb') as f:
        script = f.read()

    rc = subprocess.call(script, shell=True)


def test_timechops():
    from src.pipeline.generate_timesplits import get_time_splits

    temporal_config = {
        'feature_start_time': '2011-01-01',
        'feature_end_time': '2020-01-01',
        'label_start_time': '2016-01-01',
        'label_end_time': '2021-01-01',
        'model_update_frequency': '1month',
        'max_training_histories': ['2y'],
        'test_durations': ['1month'],
        'training_as_of_date_frequencies': ['2week'],
        'test_as_of_date_frequencies': ['2week'],
        'label_timespans': ['1y']
    }


    chopper = Timechop(
        feature_start_time=temporal_config['feature_start_time'],
        feature_end_time=temporal_config['feature_end_time'],
        label_start_time=temporal_config['label_start_time'],
        label_end_time=temporal_config['label_end_time'],
        model_update_frequency=temporal_config['model_update_frequency'],
        training_as_of_date_frequencies=temporal_config['training_as_of_date_frequencies'],
        max_training_histories=temporal_config['max_training_histories'],
        training_label_timespans=temporal_config['label_timespans'],
        test_as_of_date_frequencies=temporal_config['test_as_of_date_frequencies'],
        test_durations=temporal_config['test_durations'],
        test_label_timespans=temporal_config['label_timespans'],
    )

    visualize_chops(
        chopper=chopper,
        show_as_of_times=True,
        show_boundaries=True,
        save_target='aclu_exp.png'
    )

    timechop = chopper.chop_time()

    # print(timechop)
    # print(timechop[-1].keys())
    print('Testing...')
    print(timechop[-1])
    print(timechop[0])
    # print(timechop[0]['test_matrices'][1])



def create_session_dates_file_for_class():
    folder = '/mnt/data/db_backups/aclu_class_dump_202008'
    fn = 'regular_session_dates.csv'
    db_con = get_db_conn(fpath)
    cur = db_con.cursor()

    csv_path = os.path.join(folder, fn) 

    with open(csv_path, 'w') as f:
        # cur.copy_to(f, 'clean.session_dates', sep=',', null=None)
        cur.copy_expert("COPY clean.session_dates TO STDOUT DELIMITER ',' CSV HEADER", f)

    # db_con.commit()

def random_string_tests():
    s = 's3://aclu-leg-tracker/folder1/folder2'
    # s = s[5:]

    s = s.lstrip('s3://{}/'.format(S3_BUCKET))

    print(s)



if __name__ == '__main__':
    for i in range(1000000):
        x = 0 if i==0 else x+i

    # random_string_tests()
    # test_timechops()
    # create_session_dates_file_for_class()
    # upload_bill_labels()
    # test_bill_labeling()
    # generate_cloud_country()
    # fetch_repro_bill_texts()
    # fetch_all_bill_docs()
    # test_bill_labeling()
    # upload_results_csv_to_postgres()
    # test_bash_script()
    
    # test_es_ingestion()
    # test_bill_doc_creation()
    # test_extracting_bill_content()
    # test_extract_datasets()
    # test_download_file()  
    # test_dataset_decode()
    # test_dataset_upload()
    # test_datasets()
    # test_bill_decoding()
    # test_loader_script()
    # test_db_pop()
    # test_sessions()


