import os
import pandas as pd
import numpy as np
import logging
import psycopg2

from typing import Dict

from src.utils.general import get_db_conn


creds_folder = '../../conf/local/'
fpath = os.path.join(creds_folder, 'credentials.yaml')
db_conn = get_db_conn(fpath)

csv_file = '../../data/handling_missing_dates_state_sessions_20201020.csv'


def update_row(data_row, db_conn):
    """Update a row of the session_dates table"""

    # Some states dont have regular sessions in some years
    # And if one value is missing we are skipping
    if data_row['convene_date'] is None or data_row['adjourn_date'] is None:
        logging.info('No session dates. Skipping row {}'.format(data_row['session_entry_id']))

        return

    q = """
            update 
                clean.session_dates 
            set 
                convene_date='{}', adjourn_date='{}'
            where session_entry_id={}
        """.format(data_row['convene_date'], data_row['adjourn_date'], data_row['session_entry_id'])
    
    logging.info('Updating the row with entry_id {}'.format(data_row['session_entry_id']))

    cursor = db_conn.cursor()
    try:
        cursor.execute(q)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        return


def update_session_dates_table(db_conn, csv_file):
    """ Update the session_dates table
        Args:
            csv_file: The csv file that contains the dates to update. Should contain the row indicator (session_entry_id)
    """

    logging.info('Reading the CSV file')
    df = pd.read_csv(csv_file)

    # Converting NaNs to None
    df = df.where(pd.notnull(df), None)

    df.apply(update_row, axis=1, **{'db_conn': db_conn})

    db_conn.commit()


if __name__ == '__main__':
    update_session_dates_table(db_conn, csv_file)
