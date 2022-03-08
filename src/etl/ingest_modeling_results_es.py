import os
import sys
import logging
import json
import boto3
import hashlib
import pandas as pd
import numpy as np

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from datetime import datetime

from src.utils.general import get_elasticsearch_conn, get_db_conn

"""
    This script indexes the modeling results in Elasticsearch. 
    This index is used for the Kibana UI

    A document contains scores for a (bill_id, as_of_date) pair
    The document_id would be a hash of the (bill_id + prediction_date)

    Assume that this script would be executed right after moeling predict_forward scripts

    Index Structure: {
        "bill_id": int,
        "prediction_date": date,
        "bill_number": str,
        "doc_id": int,
        "title": str
        "descriptions": str 
        "state_name": str
        "state_loc": {'lat': float, 'long': float}
        "party": str,
        "bill_type": str,
        "introduced_date",
        "longterm_passage_score",
        "longterm_rank_pct",
        "shortterm_passage_score",
        "shortterm_rank_pct",
        "legiscan_link",
        "state_link"
        "issue_area_scores": {
            voting_rights,
            reproductive_rights,
            crimiallaw
        }
    }

"""

def _create_es_doc_hash(bill_id, prediction_date):
    """Create a hash that is used as the document id in elasticsearch index"""

    s = str(bill_id) + prediction_date 

    hash_object = hashlib.md5(s.encode())
    hex_hash = hash_object.hexdigest()

    return hex_hash


def _fetch_information_from_db(engine, prediction_date, label_timespans=None):
    """Fetching model scores and meta data for the index"""

    q = """
        with bill_scores as (
            select
                bill_id,
                to_char(as_of_date, 'YYYY-MM-DD') as as_of_date,
                score as score,
                rank_pct
            from deploy.passage_predictions
            where as_of_date='{prediction_date}'
            and label_timespan='365day'
        ),
        sponsor_party as (
            select 
                bill_id,
                case when count(distinct party_id) > 1 then 'Bipartisan' else max(party_name) end as sponsor_party
            from bill_scores left join clean.bill_sponsors using(bill_id)
            join catalogs.political_party using(party_id)
            group by 1
        ),
        texts as (
            select 
                distinct on (bill_id) 
                bill_id, 
                doc_id, 
                doc_type, 
                to_char(doc_date, 'YYYY-MM-DD') as doc_date 
            from bill_scores left join clean.bill_docs using(bill_id)
            where doc_date < '{prediction_date}'
            order by bill_id, doc_date
        )
        select
            bill_id,
            as_of_date,
            coalesce(doc_id, -1) as doc_id,
            coalesce(doc_type, 'none') as doc_type, 
            coalesce(doc_date, '1970-01-01') as doc_date,
            bill_number,
            url as legiscan_link,
            url as state_link,
            d.state as state_name,
            d.latitude as lat,
            d.longitude as lon,
            session_id,
            sponsor_party,
            session_title as session_name,
            e.description as bill_type, 
            'title' as bill_title,
            'description' as bill_description,
            score,
            rank_pct
        from bill_scores a join texts using (bill_id) 
            join sponsor_party using (bill_id)
                join clean.bills c using(bill_id) 
                    join clean.sessions s using(session_id)
                        left join catalogs.states d using(state_id) 
                                left join catalogs.bill_types e using(bill_type) 
    """.format(
        prediction_date=prediction_date
    )
    
    df = pd.read_sql(q, engine)

    return df.to_dict('record')


def _store_results_one_bill_es(es_connection, bill_scores, index):
    """Write one bill to elasticsearch
    
    Args:
        es_connection: Connection to elasticsearch
        bill_scores (Dict): A dictionary that contains the scores and other details of the bill 
        index (str): The name of the elasticsearch index we write results to
    """

    document_id = _create_es_doc_hash(
        bill_id=bill_scores['bill_id'], 
        prediction_date=bill_scores['as_of_date']
    )

    # Check whether the file hash already exists in the index
    r = es_connection.exists(index, id=document_id)

    json_object = {
        "bill_id": bill_scores.get('bill_id'),
        "as_of_date": bill_scores.get('as_of_date'),
        "doc_id": bill_scores.get('doc_id'),
        "doc_type": bill_scores.get('doc_type'),
        "doc_date": bill_scores.get('doc_date'),
        "bill_number": bill_scores.get('bill_number'),
        "bill_title": bill_scores.get('bill_title'),
        "bill_description": bill_scores.get('bill_description'),
        "bill_type": bill_scores.get('bill_type'),
        # "score_short_term": bill_scores.get('score_short_term'),
        # "rank_pct_short_term": bill_scores.get('rank_pct_short_term'),
        "score_long_term": bill_scores.get('score'),
        "rank_pct_long_term": bill_scores.get('rank_pct'),
        "legiscan_link": bill_scores.get('legiscan_link'),
        "state_link": bill_scores.get('state_link'),
        "state": bill_scores.get('state_name'),
        "state_location": {
            "lat": bill_scores.get("lat"),
            "lon": bill_scores.get("lon")
        },
        "state_location2": [bill_scores.get("lon"), bill_scores.get("lat")],
        "state_location3": "{}, {}".format(bill_scores.get("lat"), bill_scores.get("lon")),
        "session_id": bill_scores.get('session_id'),
        "session_name": bill_scores.get('session_name'),
        "sponsor_party": bill_scores.get('sponsor_party'),
        # "reproductive_rights_score": bill_scores.get('reproductive_rights'),
        # "immigrant_rights_score": bill_scores.get('immigrant_rights'),
        # "racial_justice_score": bill_scores.get('racial_justice'),
        "primary_issue_area": bill_scores.get('primary_issue_area'),
        "likelihood_long_term": bill_scores.get('likelihood_long_term'),
        "score_delta_lt": bill_scores.get('score_delta_lt'),
        "score_delta_pct_lt": bill_scores.get("score_delta_pct_lt"), 
        "score_delta_type_lt": bill_scores.get("score_delta_type_lt"),
        "bill_status": "active",
        "bill_status_date": bill_scores.get('as_of_date')
    }

    logging.info(json_object)
    es_connection.index(
        index=index, 
        id=document_id, 
        body=json.dumps(json_object)
    )

    # if not r:
    #     es_connection.index(
    #         index=index, 
    #         id=document_id, 
    #         body=json.dumps(json_object)
    #     )
    # else:
    #     logging.warning('Predictions already exist for bill_id {} and as_of_date {}. skipping'.format(
    #         bill_scores['bill_id'], bill_scores['as_of_date'])
    #     )

    # TODO: Handle the case where the file already exist. Currenty, skipping. 
    # But would be good to update the score or have the option to update the score or disregard


def _parse_search_results(es, res, statuses_to_update, status_dates_to_update):
    """parse the results response received from elastic search query search"""
    
    # The number of total hits for the query
    num_total_hits = res['hits']['total']['value']

    # The list of bills returned in the query
    hits = res['hits']['hits']

    if len(hits)> 0:
        for h in hits:
            content = h['_source']
            if content['bill_id'] in statuses_to_update:
                
                logging.info('updating the status of bill {}'.format(content['bill_id']))
                content['bill_status'] = statuses_to_update[content['bill_id']]
                content['bill_status_date'] = status_dates_to_update[content['bill_id']]

                logging.info(content)
                es.index(
                    index=h['_index'], 
                    id=h['_id'], 
                    body=json.dumps(content)
                )


    # The ID of the scroll for pagination of a larger result
    scroll_id = res.get('_scroll_id')

    return num_total_hits, hits, scroll_id


def update_status_flags(es, engine, index, as_of_date, query_size=10):
    """Update the status flag and the status date for all the active bills added before the as_of_date"""

    # TODO -- double check the query for correct status
    q = """
        with bills as (
            select 
                distinct bill_id,
                max(progress_date) as last_progress_date,
                min(('{as_of_date}'::DATE - event_date::DATE)::int) as days_since_last_event
            from deploy.passage_predictions 
                join clean.bill_progress using(bill_id)
                    join clean.bill_events using(bill_id)
            where as_of_date < '{as_of_date}' and progress_date < '{as_of_date}' and event_date < '{as_of_date}'
            group by bill_id
        )
        select 
            bill_id,
            max(days_since_last_event) as days_since_last_event,
            to_char(max(last_progress_date), 'YYYY-MM-DD') as last_status_date,
            max(case 
                    when progress_date=last_progress_date and event in (4, 5, 6) 
                    then event else 0 
                end) as last_status        
        from bills left join clean.bill_progress using(bill_id)
        group by bill_id
    """.format(as_of_date=as_of_date)

    bill_status = pd.read_sql(q, engine)

    if len(bill_status) == 0:
        return

    bill_status['status_label'] = 'active'
    bill_ids = bill_status['bill_id'].tolist()

    # set the status 
    msk = (bill_status['last_status'].isin([4, 5, 6]))
    msk2 = bill_status['days_since_last_event'] > 60

    d = {4: 'passed', 5: 'failed', 6: 'vetoed'}
    bill_status.loc[msk, 'status_label'] = bill_status[msk].apply(lambda x: d[x['last_status']], axis=1)
    bill_status.loc[~msk & msk2, 'status_label'] = 'dormant'

    # Getting the bills that have had a change in status
    msk3 = bill_status['status_label'] == 'active'
    bills_with_change = bill_status[~msk3]
    statuses_to_update = {x['bill_id']:x['status_label'] for _, x in bills_with_change.iterrows()}
    status_dates_to_update = {x['bill_id']:x['last_status_date'] for _, x in bills_with_change.iterrows()}
    # query to fetch the all docs for the bill_id where the status is active
    query = {
        "query": {
            "bool": {
            "must": [
                {
                    "terms": {
                        "bill_id": bill_ids
                    }
                },
                {
                    "terms": {
                        "bill_status": ["active", "dormant"]
                    }
                },
                {
                    "range":{
                        "as_of_date": {
                            "lt": as_of_date
                        }
                    }   
                }
            ]
            }
        }
    }

    res = es.search(index=index, body=query, scroll='1m', size=query_size)
    _, hits, scroll_id = _parse_search_results(es, res, statuses_to_update, status_dates_to_update)

    stop = False
    while not stop:
        res = es.scroll(scroll_id=scroll_id, scroll='1m')
        _, temp, scroll_id = _parse_search_results(es, res, statuses_to_update, status_dates_to_update)
        if len(temp) == 0:
            stop = True
            continue

        # hits = hits + temp

    # print(len(hits))
    # print(len(hits[0]))

    # for h in hits:
    #     content = h['_source']
    #     if content['bill_id'] in statuses_to_update:
            
    #         logging.info('updating the status of bill {}'.format(content['bill_id']))
    #         content['bill_status'] = statuses_to_update[content['bill_id']]
    #         content['bill_status_date'] = status_dates_to_update[content['bill_id']]

    #         logging.info(content)
    #         es.index(
    #             index=h['_index'], 
    #             id=h['_id'], 
    #             body=json.dumps(content)
    #         )

def get_issue_area_for_bill(engine, bill_id):
    """fetch and append the issue area label for a bill"""

    q = """
        select  
            bill_id,
            max(i.relevance_score) as immigrant_rights,
            max(c.relevance_score) as criminal_law,
            max(r.relevance_score) as racial_justice,
            max(l.relevance_score) as lgbt_rights,
            max(rep.relevance_score) as reproductive_rights
        from clean.bills 
        left join labels_es.immigrant_rights i using(bill_id)
        left join labels_es.criminal_law_reform c using(bill_id)
        left join labels_es.racial_justice r using(bill_id)
        left join labels_es.lgbt_rights l using(bill_id)
        left join labels_es.reproductive_rights rep using(bill_id)
        where bill_id = {bill_id}
        group by 1
    """.format(bill_id=bill_id)

    df = pd.read_sql(q, engine).fillna(0).set_index('bill_id')

    # TODO -- This is a hack because clean.bills seem to dropped some bills. Shouldn't be needed
    if df.empty:
        return 'other'

    if df.max(axis=1).iloc[0] == 0:
        issue_area = 'other'
    else:
        issue_area = df.idxmax(axis=1).iloc[0]
        
    return issue_area

def get_score_delta_for_bill(engine, bill_id):
    """Get the score difference for the bill"""

    # TODO -- Set the label timespan

    q = """
        select 
            * 
        from deploy.passage_predictions 
        where bill_id={bill_id} 
        order by as_of_date desc 
    """.format(
        bill_id=bill_id
    )

    df = pd.read_sql(
        q, engine
    )

    delta_type = 'None'
    pct_delta = 0

    if len(df) == 0:
        return 0

    if len(df) == 1:
        delta_abs = df.at[0, 'score']
        pct_delta = 0
        delta_type = 'New Bill'
    else:
        delta = df.at[0, 'score'] - df.at[1, 'score']

        if delta > 0:
            delta_type = 'Increased'
        elif delta < 0:
            delta_type = 'Decreased'
            # delta = abs(delta)
        else:
            delta_type = 'Unchanged'
        
        delta_abs = np.absolute(delta)
        pct_delta = (delta_abs/ df.at[1, 'score']) * 100
        
    return delta_abs, pct_delta, delta_type


def get_score_bucket_for_bill(passage_score):
    """assign a score bucket"""

    buckets = {
        '6-will_not_pass': [0, 0.10],
        '5-unlikely': [0.10, 0.35],
        '4-could go either way': [0.35, 0.55],
        '3-likely': [0.55, 0.70],
        '2-very_likely': [0.70, 0.90],
        '1-will_pass': [0.90, 1.1]
    }

    bucket_label = ''
    
    for b, lims in buckets.items():
        if passage_score < lims[1] and passage_score >= lims[0]:
            bucket_label = b
            break

    return bucket_label



def run_pipeline(prediction_date):
    creds_folder = '../../conf/local/'
    creds_file = os.path.join(creds_folder, 'credentials.yaml')

    # connections
    es = get_elasticsearch_conn(creds_file)
    db_conn = get_db_conn(creds_file)

    label_timespan_mappings = {
        'short_term': '30day', 
        'long_term': '365day'
    }   
    
    results_index = 'bill_scores_temp'

    logging.info('Fetching predictions from the DB')

    predictions = _fetch_information_from_db(
        engine=db_conn, 
        prediction_date=prediction_date, 
        label_timespans=label_timespan_mappings
    )

    logging.info('Updating status flags with new data in index {}'.format(results_index))
    
    # try:
    #     update_status_flags(es=es, engine=db_conn, index=results_index, as_of_date=prediction_date)
    # except NotFoundError as e:
    #     logging.warning('Index does not exist to update status flags. Continuing to writing')

    logging.info('Writing passage scores to elasticsearch index {}'.format(results_index))
    for pred in predictions:
        logging.info('adding aditional info for bill {}'.format(pred['bill_id']))
        pred['primary_issue_area'] = get_issue_area_for_bill(engine=db_conn, bill_id=pred['bill_id'])
        pred['likelihood_long_term'] = get_score_bucket_for_bill(pred['score'])
        pred['score_delta_lt'], pred['score_delta_pct_lt'], pred['score_delta_type_lt'] = get_score_delta_for_bill(engine=db_conn, bill_id=pred['bill_id'])

        logging.info('Writing results of bill {} to elasticsearch'.format(pred['bill_id']))
        _store_results_one_bill_es(es_connection=es, bill_scores=pred, index=results_index)


def write_temp_results_to_es():
    start_date='2021-01-01'
    # end_date='2020-09-01' 

    creds_folder = '../../conf/local/'
    creds_file = os.path.join(creds_folder, 'credentials.yaml')
    db_conn = get_db_conn(creds_file)

    q = """
        select 
            to_char(prediction_date, 'YYYY-MM-DD') as prediction_date
        
        from (
            select 
                distinct as_of_date as prediction_date
            from deploy.passage_predictions 
            where as_of_date > '{date_cutoff}'
            order by as_of_date 
        ) as t
    """.format(
        date_cutoff=start_date
    )

    df = pd.read_sql(q, db_conn)

    prediction_dates = df['prediction_date'].tolist()

    for pred_date in prediction_dates:
        run_pipeline(pred_date)


# if __name__ == '__main__':
#     # Logger
#     # timestr = datetime.now().strftime("%y%m%d%H%M%S")   
#     logging.basicConfig(level=logging.INFO, filename=f"../../logs/results_to_es.DEBUG", filemode='w')
#     logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
    
#     write_temp_results_to_es()

