import os
import sys
import logging

import pandas as pd

from src.utils.general import (
    get_db_conn, 
    get_boto3_session, 
    get_elasticsearch_conn, 
    load_matrix_s3, load_model_s3
)

creds_folder = '../../conf/local/'
credentials_file = os.path.join(creds_folder, 'credentials.yaml')

date = '2020-06-01'

def _fetch_passage_scores(engine, prediction_date, labelspans={'short': '30 days', 'long': '1y'}):
    """Short term and long term passage scores"""

    # short term
    q = """
        select 
            bill_id, 
            to_char(as_of_date, 'YYYY-MM-DD') as prediction_date,
            score as short_term_passage_likelihood
        from deploy.temp_passage_scores
        where as_of_date='2020-06-01'
        and label_timespan='30 days'
    """

    short_term_df = pd.read_sql(q, engine)

    print(short_term_df)

    q = """
        select 
            bill_id, 
            to_char(as_of_date, 'YYYY-MM-DD') as prediction_date,
            score as long_term_passage_likelihood
        from deploy.temp_passage_scores 
        where as_of_date='2020-06-01'
        and label_timespan='365 days'
    """

    long_term_df = pd.read_sql(q, engine)

    print(long_term_df)

    df = short_term_df.merge(long_term_df, on=['bill_id', 'prediction_date'], how='left')

    return df


def _fetch_bill_info(engine, prediction_date):
    # short term
    q = """
        select 
            bill_id, 
            max(to_char(as_of_date, 'YYYY-MM-DD')) as prediction_date,
            max(b.bill_type),
            max(b.subjects),
            max(b.url) as legiscan_url,
            max(d.state) as state,
            max(c.session_title) as session_name
        from deploy.temp_passage_scores a 
            join clean.bills b using (bill_id) 
                join clean.sessions c using (session_id) 
                    join catalogs.states d using(state_id)
        where as_of_date='2020-06-01'
        group by bill_id
    """

    df = pd.read_sql(q, engine)

    return df

def _fetch_issue_area_scores(engine, prediction_date, issue_area):
    pass


def generate_spreadsheet(engine, prediction_date):

    labelspans = {'short': '1month'}

    output = pd.DataFrame()

    passage_scores = _fetch_passage_scores(
        engine=engine,
        prediction_date=prediction_date,
        labelspans=labelspans
    )

    bill_info = _fetch_bill_info(
        engine=engine,
        prediction_date=prediction_date
    )

    output = bill_info.merge(passage_scores, on=['bill_id', 'prediction_date'], how='left')


if __name__ == '__main__':
    db_conn = get_db_conn(credentials_file)
    generate_spreadsheet(
        engine=db_conn,
        prediction_date='2020-06-01'
    )






