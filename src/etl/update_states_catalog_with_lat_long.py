## This script adds the lat/long values of each state to the DB
## Assumes that the table is already populated with the state_id, abbreviation, and state; and the columns latitude/longitude exist in the DB
import os
import pandas as pd
import psycopg2
import logging

from src.utils.general import get_db_conn

creds_folder = '../../conf/local/'
fpath = os.path.join(creds_folder, 'credentials.yaml')
db_con = get_db_conn(fpath)

# Downloaded from: https://developers.google.com/public-data/docs/canonical/states_csv
lat_long_values_file = '../../data/state_lat_long_values.csv'

logging.info('Fetching the lat/long values for states from file {}'.format(lat_long_values_file))
lat_long_df = pd.read_csv(lat_long_values_file)

# Fetching the current states catalog table

logging.info('Mapping the lat/long values to the state_ids')
q = """
    select 
        state_id, state_abbreviation, state as state_name
    from catalogs.states
"""

states_df = pd.read_sql(q, db_con)

# joining the lat long values
# Note: This is done this way to persit with the state ID used by legiscan
# Using a pretty roundabout to get to the correct column names
columns = ['state_id', 'state_abbreviation', 'state_name', 'latitude', 'longitude']
states_df = states_df.merge(lat_long_df, left_on='state_abbreviation', right_on='state', how='left')[columns]
states_df.rename(columns={'state_name': 'state'}, inplace=True)

# DF as a lsit of tuples for writing to the db
tpl = [tuple(x) for x in states_df.to_numpy()]

# We truncate the existing table and then write the new records
q1 = "TRUNCATE TABLE catalogs.states"

q2 = "INSERT INTO catalogs.states ({}) VALUES ({})".format(
    ', '.join(list(states_df.columns)),
    ', '.join(['%s'] * states_df.shape[1])
)

logging.info('Updating the table with lat/long values')

try:
    cursor = db_con.cursor()
    cursor.execute(q1)
    cursor.executemany(q2, tpl)
    db_con.commit()
except (Exception, psycopg2.DatabaseError) as error:
    logging.error(error)
    raise psycopg2.DatabaseError(error)

logging.info('Successfully updated the catalogs.states table with lat/long tables')











