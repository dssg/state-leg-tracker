from os import name
import psycopg2
import logging
from src.pipeline.legiscan_updates import TODAY

from src.utils.general import get_db_conn
from src.bill_passage.feature_generation.calculate_sponsor_success_rates import calculate_bill_sponsor_success_rates

"""This script syncs up the clean schema of the DB with the elastic search indexes by droppping clean tables and recreating them"""

CLEAN_TABLES = {
    'bills': ['bill_id'], 
    'bill_docs': ['bill_id', 'doc_id'],
    'bill_events': ['bill_id', 'event_hash'],
    'bill_progress': ['bill_id', 'progress_date', 'event'],
    'bill_votes': ['vote_id', 'bill_id'],
    'sessions': ['session_id'],
    'session_people': ['session_id', 'people_id'],
    'bill_committees': ['bill_id', 'committee_id'],
    'bill_amendments': ['bill_id', 'amendment_id'],
    'bill_sponsors': ['sponsor_id', 'bill_id']
}

def _compare_current_and_new(db_conn):
    """
        Checking whether the new dataset contains everything from the past to avoid erasing our datasets
    """

    # The table names and their primary key fields
    tables = {
        'bills': ['bill_id'], 
        'bill_docs': ['bill_id', 'doc_id'],
        'bill_events': ['bill_id', 'event_hash'],
        'bill_progress': ['bill_id', 'progress_date', 'event'],
        'bill_votes': ['vote_id', 'bill_id'],
        'sessions': ['session_id'],
        'session_people': ['session_id', 'people_id'],
        'bill_committees': ['bill_id', 'committee_id'],
        'bill_amendments': ['bill_id', 'amendment_id'],
        'bill_sponsors': ['sponsor_id', 'bill_id']
    }

    current_schema = 'clean'
    new_schema ='clean_new'

    checks_passed = True
    cursor = db_conn.cursor()

    # Let's calculate the number of rows that currently exist in each table as it is useful for all the checks we do
    n_rows_curr = dict()
    for table in tables.keys():
        q = f"select count(*) from {current_schema}.{table}"
        cursor.execute(q)
        n_rows_curr[table] = cursor.fetchone()[0]


    # 1. Check the number of rows between table pairs. The new one should be >= the old
    logging.info('Comparing the number of rows in the old and the new versions of each table...')
    for table in tables.keys():
        q = f"select count(*) from {new_schema}.{table}"
        cursor.execute(q)
        n_rows_new = cursor.fetchone()[0]

        if n_rows_new < n_rows_curr[table]:
            logging.error(
                'The table with new data has fewer rows ({n_rows_new}) \
                than the current table ({n_rows_old}) in the {table_name} table'.format(
                    n_rows_new=n_rows_new, n_rows_old=n_rows_curr, table_name=table
                )
            )
            checks_passed = False

    if not checks_passed:
        logging.error('We are not moving ahead with the more expensive checks since the first one failed!')
        return checks_passed

    logging.info('The row numbers check out! Now, let us compare the actual content')
    # If the previous check passed, we move on to the next level
    # 2. Check whether the new table has everything from the past
    # The inner join should have the same row count as the old table
    for table, pkey in tables.items():
        q="""
            select count(*) 
            from {current_schema}.{table_name} inner join 
            {new_schema}.{table_name} using({pkey})            
        """
        cursor.execute(
            q.format(
                table_name=table,
                current_schema=current_schema,
                new_schema=new_schema,
                pkey=','.join(pkey)
            )
        )
        n_recs_joined = cursor.fetchone()[0]

        if n_recs_joined != n_rows_curr[table]:
            logging.warning('The new table does not contain everything from the old for {}'.format(table))
            # checks_passed=False

    if not checks_passed:
        logging.error('We are not moving ahead with the next check(s)!')
        return checks_passed

    logging.info('The content check out relative to the primary keys. Lets go further and check whether all the rows match too')

    for table in tables.keys():
        q = f"""
            with curr_table as (
                select md5(a.*::text) as col_hash from {current_schema}.{table} a
            ),
            new_table as (
                select md5(b.*::text) as col_hash from {new_schema}.{table} b
            )
            select 
                count(*)
            from curr_table left join new_table using(col_hash) 
        """
        cursor.execute(q)
        n_recs_hashed_joined = cursor.fetchone()[0]

        # The hashed and joined table should have the same number of rows as the old one
        # currently, not returning False if the hashes don't match. Just generates a warning
        if n_recs_hashed_joined != n_rows_curr[table]:
            logging.warning('In table {}, {} rows have updated values..'.format(table, abs(n_recs_hashed_joined-n_rows_curr[table])))
            # Not setting the flag to false for now
            # checks_passed=False

    return checks_passed


def sync_db_with_es():
    """
        Syncing the database with elasticsearch.
        The process:
            1. Fetch the new data into a schema named clean_new
            2. Perform some checks to ensure we have everything from current clean schema in the clean_new
            3. Drop the current clean schema and rename clean_new to clean. 
            4. If the checks didn't pass, rename clean_new to clean_bad and leave for inspection
    """
    db_conn = get_db_conn('../../conf/local/credentials.yaml')
    cursor = db_conn.cursor()

    # TODO -- add NCSL table
    populate_clean_tables_script = '../../sql/populate_from_es_to_postgres_clean.sql'

    # scripts for pre-calculated tables for triage
    chamber_control_script = '../../sql/create_populate_session_chamber_control_table.sql'

    logging.info('Creating the clean_new schema to fetch the updated data...')

    clean_new_schema_q = """
        set role rg_staff;

        drop schema if exists clean_new cascade;

        create schema clean_new;
    """

    # Creating the identical clean_new schema
    for table_name in CLEAN_TABLES.keys():
        q = f"\ncreate table clean_new.{table_name} (like clean.{table_name} including all);"

        clean_new_schema_q += q

    cursor.execute(clean_new_schema_q)
    db_conn.commit()


    logging.info('Tables in the new clean schema created. Starting data population')

    try:
        with open(populate_clean_tables_script, 'r') as script:
            # We can use the same populate script we use for populating clean tables, but with an updated schema name
            txt = script.read().replace('clean', 'clean_new')
            cursor.execute(txt)
            db_conn.commit()
    except psycopg2.DatabaseError as error:
        logging.error(error)
        db_conn.commit()
        # db_conn.rollback()
        raise error

    logging.info('Populated the new tables. Starting the comparison between new and old to make sure we have not lost any data')
    checks_passed = _compare_current_and_new(db_conn)

    if checks_passed:
        logging.info('Content check passed!')
        logging.info('Setting the newly fetched data as the clean schema')

        q = """
            drop schema clean cascade;

            alter schema clean_new rename to clean;
        """

        try:
            cursor.execute(q)
            db_conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            raise error

        logging.info('Successfully populated the tables! Now on to creatng the precalculated tables for triage')

        logging.info('First up is the table with the controlling party for each session, chamber pair')
        try:
            with open(chamber_control_script, 'r') as script:
                cursor.execute(script.read())
            db_conn.commit()
        except psycopg2.DatabaseError as error:
            raise error

        logging.info('Controlling parties calculated! Now on to calculating bill sponsor success rates')
        calculate_bill_sponsor_success_rates(db_conn)

        logging.info('Success rates calculated!')

        logging.info('Data syncing is complete!')
    else:
        # TODO: A better name for the clean_bad
        logging.error('The integrity checks did not pass! The data update unsuccessful')
        logging.warning('Keeping the new schema for comparison. Renaming to clean_bad!')

        q = """
            drop schema if exists clean_bad cascade;

            alter schema clean_new rename to clean_bad;

            -- Have to take care of the session and bill hashes tables
            delete from legiscan_update_metadata.bill_hashes
            where update_date = '{today}';

            delete from legiscan_update_metadata.session_hashes
            where update_date = '{today}'
        """.format(today=TODAY)

        try:
            cursor.execute(q)
            db_conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            raise error

    return checks_passed


if name == '__main__':
    sync_db_with_es()



    

