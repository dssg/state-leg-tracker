import sys
import pandas as pd
import logging
import psycopg2

from src.utils.general import get_db_conn, copy_df_to_pg

# logging.basicConfig(level=logging.DEBUG, filename="../../logs/calculating_sponsor_success.DEBUG", filemode='w')
# logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging.basicConfig(level=logging.DEBUG)


"""
This script calculates the past success rate of each sponsor as of a certain date. 
Ideally the feature should be calculated at every as_of_date in the timechop.
But as a first pass, calculating the feature for each bill introduction date
"""


def create_temp_table(db_conn):
    # creating the table
    table_q = """
        CREATE SCHEMA IF NOT EXISTS pre_triage_features;

        CREATE TABLE IF NOT EXISTS pre_triage_features.temp_sponsor_success (
            sponsor_id int,
            knowledge_date timestamp,
            num_bills_sponsored int,
            num_bills_passed int,
            success_rate numeric(6,5),
            PRIMARY KEY (sponsor_id, knowledge_date)
        );
    """

    cursor = db_conn.cursor()
    try: 
        cursor.execute(table_q)
        db_conn.commit()  
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _fetch_knowledge_dates(db_conn):
    # The knowledge dates -- We calculate the metric at each bill introduction date 
    # Using a '2000-01-01' cut off as some bills with intro dates of 1969

    # Fetching the knowledges not present in the sponsor success table to avoid repeated calculations
    # There are bills in the data with intro dates in the future, adding a where clause to filter only the dates upto today
    q = """
        with last_knowledge_date as (
            select 
                max(knowledge_date) as last_date 
            from pre_triage_features.temp_sponsor_success
            where knowledge_date < current_date
        ) 
        select 
            distinct introduced_date from clean.bills, last_knowledge_date
        where introduced_date > last_date
        order by introduced_date
    """

    logging.info('Fetching the distinct knowledge dates')
    knowledge_dates = pd.read_sql(q, db_conn)['introduced_date'].to_list()

    return knowledge_dates


def _data_already_exists(db_conn, knowledge_date):
    check_date_exist_q = """
        select count(*) from pre_triage_features.temp_sponsor_success where knowledge_date='{}' 
    """

    q = check_date_exist_q.format(knowledge_date)
    num_rows = pd.read_sql(q, db_conn).at[0, 'count']

    if num_rows > 0:
        return True
    else:
        return False


def create_joined_table(db_conn):
    """
    Creates a table where there's a row for each bill_id, sponsor_id, knowledge_date
    """

    logging.info('creating a joined table between the bill_sponsors and the success rates table to avoid an expensive join during triage experiment')

    q = """
        DROP TABLE IF EXISTS pre_triage_features.bill_sponsor_success;

        create table pre_triage_features.bill_sponsor_success as (
        with bill_spnsr_info as (
            select bill_id, introduced_date, sponsor_id from clean.bills a join clean.bill_sponsors b using(bill_id)
        )
        select 
            bill_id,
            a.sponsor_id,
            knowledge_date,
            num_bills_sponsored,
            num_bills_passed,
            success_rate
        from bill_spnsr_info a join pre_triage_features.temp_sponsor_success b on (a.sponsor_id=b.sponsor_id and a.introduced_date=b.knowledge_date)
    )
    """

    cursor = db_conn.cursor()
    try: 
        cursor.execute(q)
        db_conn.commit()  
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        raise error

    logging.info('Created the joined table!')

    q1 = "alter table pre_triage_features.bill_sponsor_success add primary key (bill_id, sponsor_id)"
    q2 = "CREATE INDEX knwldge_date_index ON pre_triage_features.bill_sponsor_success(knowledge_date)"

    cursor = db_conn.cursor()
    try: 
        cursor.execute(q1)
        cursor.execute(q2)
        db_conn.commit()  
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    logging.info('Added the primary key and the index for the date')


def calculate_bill_sponsor_success_rates(db_conn):
    """
        Main function for calculating the sponsor success rates. 
        Calculates the success rate of a sponsor at each time they introduced a bill.
        Creates two tables:
            1) temp_sponsor_success (sponsor_id, knoledge_date, success_rate)
            2) bill_sponsor_success (bill_id, sponsor_id, knowledge_date, success_rate)
        bill_sponsor_success table is used in triage
    """

    create_temp_table(db_conn)
    knowledge_dates = _fetch_knowledge_dates(db_conn)

    calculation_q = """
        with sponsors_scored as (
            select 
                sponsor_id
            from clean.bills left join clean.bill_sponsors using(bill_id)
            where introduced_date='{knowledge_date}'
            group by 1
        ),
        sponsor_history as (
            select 
                bill_id, sponsor_id, max(case when event=4 then 1 else 0 end) as passed
            from clean.bills left join clean.bill_progress using(bill_id) 
                left join clean.bill_sponsors using(bill_id) 
                    join sponsors_scored using(sponsor_id)
            where introduced_date < '{knowledge_date}' and progress_date < '{knowledge_date}'
            group by 1, 2
            order by 1
        )
        select
            sponsor_id, 
            '{knowledge_date}'::date as knowledge_date,
            count(distinct bill_id) as num_bills_sponsored,
            sum(passed) as num_bills_passed,
            sum(passed)::float / count(distinct bill_id) as success_rate
        from sponsor_history
        group by sponsor_id
    """

    for i, knowledge_date in enumerate(knowledge_dates):
        logging.info('Calculating the success rates at {}. ({} out of {})'.format(knowledge_date, i+1, len(knowledge_dates)))

        if _data_already_exists(db_conn, knowledge_date):
            # NOTE: The assumption here is that the if the knowledge data exist, success rate for all the sponsors at that time exist. 
            # Could be handled better
            logging.warning('The calculations for knowledge date {} already exists in the DB. Skipping'.format(knowledge_date))
            continue
    
        q = calculation_q.format(knowledge_date=knowledge_date)
        df = pd.read_sql(q, db_conn)

        df['knowledge_date'] = knowledge_date

        logging.info('Copying results to the DB')

        try:
            copy_df_to_pg(
                engine=db_conn,
                table_name='pre_triage_features.temp_sponsor_success',
                df=df,   
            )
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)
            logging.warning('Skipping as the sponsor_id, knowledge_pair already exist')

    logging.info('Calculated the success rates of sponsors as of {} different knowledge days'.format(len(knowledge_dates)))

    create_joined_table(db_conn)


if __name__ == '__main__':
    cred_file = '../../../conf/local/credentials.yaml'
    db_conn=get_db_conn(cred_file)
    calculate_bill_sponsor_success_rates(db_conn)

