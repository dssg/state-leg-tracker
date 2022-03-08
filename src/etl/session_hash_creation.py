import boto3
import pickle
import logging

from datetime import date
from psycopg2 import extras, DatabaseError

from src.utils.general import get_s3_credentials, get_db_conn

import src.utils.project_constants as constants

TODAY = date.today()


def populate_session_hashes():
    """
    Baseline of the session hashes
    :return:
    """
    s3_creds = get_s3_credentials("../../conf/local/credentials.yaml")

    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )
    s3 = session.resource('s3')
    s3_bucket = constants.S3_BUCKET
    s3_file_name = "datasetlist.pkl"
    s3_path = "/".join([constants.SESSION_HASHES, s3_file_name])

    # get the pickle with session hashes obtained for the 31 july 2020!.
    s3.meta.client.download_file(s3_bucket, s3_path, s3_file_name)
    datasetlist = pickle.load(open(s3_file_name, 'rb'))

    # get the required fields to store in db
    current_session = []
    for elements in datasetlist['datasetlist']:
        current_session.append((elements['session_id'],
                                elements['state_id'],
                                elements['dataset_hash'],
                                TODAY))

    # store in db
    db_conn = get_db_conn('../../conf/local/credentials.yaml')
    cursor = db_conn.cursor()

    q = """
        INSERT INTO legiscan_update_metadata.session_hashes (session_id, state_id, session_hash, update_date) 
        VALUES (%s,%s,%s,%s);
    """

    # Writing database entry
    try:
        logging.info('writing dataset entry to the database')
        extras.execute_batch(cursor, q, current_session)
        db_conn.commit()
    except (Exception, DatabaseError) as error:
        logging.error(error)


populate_session_hashes()