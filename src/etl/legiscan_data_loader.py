import logging
import psycopg2

from psycopg2.extensions import connection

from src.etl.populate_table_functions import populate_sessions, populate_bills


def fresh_load(db_conn: connection, api_key: str):
    """ Loads all sessions, and all bills for all states assuming clean start"""

    cursor = db_conn.cursor()
    # Populating sessions for all states
    logging.info('Fetching states')
    q = " select state_abbreviation from clean.states"
    try: 
        
        cursor.execute(q)
        states = cursor.fetchall()
        states = [x[0] for x in states] # results returned as a tuple
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        return 

    logging.debug(states)

    logging.info('Writing sessions')
    populate_sessions(db_conn, api_key, states)

    # Bills
    q = "select distinct session_id from raw.sessions"
    try:
        cursor = db_conn.cursor()
        cursor.execute(q)
        sess_ids = cursor.fetchall()
        sess_ids = [x[0] for x in sess_ids]
        cursor.close()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        return

    logging.info('Writing bills')
    populate_bills(db_conn, api_key, sess_ids)



