import os
import sys
project_path = '.'
sys.path.append(project_path)

import logging
logging.basicConfig(level=logging.DEBUG, filename="log_legiscan_data_loader.debug", filemode='w')

from src.utils.general import get_legiscan_key, get_db_conn
from src.etl.legiscan_data_loader import fresh_load

creds_folder = 'conf/local/'
fpath = os.path.join(creds_folder, 'credentials.yaml')


def run_data_loader():
    """Runing the script to load data to the database from a clean start"""
    key = get_legiscan_key(fpath)
    db_con = get_db_conn(fpath)

    fresh_load(db_con, key)


if __name__ == '__main__':
    run_data_loader()
