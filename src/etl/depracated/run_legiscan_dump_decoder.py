import os
import sys
project_path = '.'
sys.path.append(project_path)

import logging
import boto3
import time

from datetime import datetime


timestr = datetime.now().strftime("%y%m%d%H%M%S")
logging.basicConfig(level=logging.INFO, filename=f"logs/legiscan_dump_decoder_{timestr}.DEBUG", filemode='w')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

from src.utils.general import get_s3_credentials
from src.etl.legiscan_dump_parser import parse_legiscan_dump

creds_folder = 'conf/local/'
fpath = os.path.join(creds_folder, 'credentials.yaml')


def run_parsing_legiscan_dump():
    s3_creds = get_s3_credentials(fpath)
    st = time.perf_counter()
    parse_legiscan_dump(
        s3_creds, 
        'legiscan_dump_20200615', 
        'extracted_bill_docs_parallelized', 
        n_jobs=-1
    )
    en = time.perf_counter()
    logging.info('parallel time {}'.format(en-st))


if __name__ == '__main__':
    run_parsing_legiscan_dump()
