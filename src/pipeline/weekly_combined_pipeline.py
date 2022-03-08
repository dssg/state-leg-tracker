#! /home/kasun/.pyenv/versions/aclu/bin/python

""" This script runs the data refresh from Legiscan, passage prediction, and updating modeling scores on ES
    Running this pipeline weekly would keep the public access tool updated. 
    This script is set as an executable (775) and run through a cron job. 
"""
import sys
import logging
import psycopg2

sys.path.append('../../')

from datetime import date

from src.utils.general import get_db_conn
from src.pipeline.legiscan_updates import update_data_from_legiscan
from src.etl.sync_db_with_es import sync_db_with_es
from src.etl.scrapers_for_session_calendars import multistate_scraper, write_to_session_dates_table
from src.bill_passage.predict_forward.predict_forward import run_pipeline as passage_prediction
from src.etl.ingest_modeling_results_es import run_pipeline as ingest_predictions_to_es

TODAY = date.today()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
fh = logging.FileHandler('../../logs/weekly_full_pipeline_{}.log'.format(TODAY), mode='w')
# fh = logging.FileHandler('../../logs/testing_pipeline_components.log'.format(TODAY), mode='w')
fh.setFormatter(formatter)
logger.addHandler(fh)

# ch = logging.StreamHandler()
# ch.setFormatter(formatter)
# logger.addHandler(ch)
# logging.basicConfig(level=logging.INFO, filename="../../logs/weekly_full_pipeline{}.log".format(TODAY), filemode='w')

EXPERIMENT_CONFIG = '../bill_passage/triage_config/national_long_term_predict_forward.yaml'

PROJECT_PATH = 's3://aclu-leg-tracker/experiment_data/bill_passage/predict_forward'

CREDENTIALS_FILE = '../../conf/local/credentials.yaml'

SQL_CREATE_SESSION_DATES_TABLE = '../../sql/linking_ncsl_and_legiscan_session_data.sql'

SQL_UPDATE_SESSION_DATES = '../../sql/adjust_session_adjourn_times.sql'


logger.info('Starting to update Elasticsearch with the weekly snapshot from LegiScan...')
update_data_from_legiscan()

# TODO -- Condition the sync DB function on whether any data are updated from legiscan

logger.info('Sync database with Elasticsearch indexes...')
checks_passed = sync_db_with_es()

logging.debug('Checks Passed? {}'.format(checks_passed))

if checks_passed:
    logger.info('Scraping multistates.us to look for any changes to legislative session calendars')
    db_conn = get_db_conn(CREDENTIALS_FILE)
    df = multistate_scraper(
        session_year=TODAY.year
    )

    write_to_session_dates_table(
        db_conn, df, TODAY.year
    )
    cursor = db_conn.cursor()
    try:
        with open(SQL_CREATE_SESSION_DATES_TABLE, 'r') as script:
            cursor.execute(script.read())
        db_conn.commit()
    except psycopg2.DatabaseError as error:
        raise error

    logger.info(
        'The session adjourn dates from multistates is not reliable. \
        That can lead us to miss some active sessions. Using our data to update unreliable session adjourn dates'
    )
    logger.debug('This is written to a pretriage features table while retaining the above created one')

    try:
        with open(SQL_UPDATE_SESSION_DATES, 'r') as script:
            cursor.execute(script.read())
        db_conn.commit()
    except psycopg2.DatabaseError as error:
        raise error

    logger.info('Data update complete!')
    # TODO -- Add some summary stats about how many sessions/states, new bills, changes to existing bills...

    logger.info('Retraining passage models and generating scores...')
    passage_prediction(
        credentials_file=CREDENTIALS_FILE,
        config_file=EXPERIMENT_CONFIG,
        project_path=PROJECT_PATH,
        prediction_date=TODAY,
        n_jobs=8
    )

    logger.info('Writing the generated scores to Elasticsearch')
    ingest_predictions_to_es(
        prediction_date=TODAY
    )

    logger.info('Pipeline for {} is completed successfully. You can visualize the results on Kibana!'.format(TODAY))

else:
    logging.error('The data update was not successful. Check clean_bad for the fetched data. Aborting!')