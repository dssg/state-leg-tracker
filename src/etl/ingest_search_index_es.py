import os
import sys
import logging
import json
import boto3
import hashlib
import pandas as pd

from elasticsearch import Elasticsearch
from datetime import datetime

from src.utils.general import get_elasticsearch_conn, get_db_conn


def _get_issues(issue_scores):
    issues = list()
    for issue_area, score in issue_scores.items():
        if score==1:
            issues.append(issue_area)
    
    if not issues:
        return 'not available'
    else:
        return ', '.join(issues)

def _create_es_doc_hash(bill_id, doc_id):
    """Create a hash that is used as the document id in elasticsearch index"""

    s = str(bill_id) + str(doc_id) 

    hash_object = hashlib.md5(s.encode())
    hex_hash = hash_object.hexdigest()

    return hex_hash

def _fetch_details_from_db(engine):
    q = """
        with sessions as (
            select session_id, session_title, state, state_abbreviation, latitude, longitude from clean.sessions join catalogs.states using(state_id) 
            where year_start >= 2019
        ),
        bill_meta as (
            select
                bill_id,
                bill_number,
                legiscan_link,
                state_link,
                bill_title,
                bill_description,
                to_char(introduced_date, 'YYYY-MM-DD') as introduced_date,
                s.*
            from clean.bills2 a join sessions s using(session_id) join clean.bills b using(bill_id) 
            where introduced_date >= '2020-05-01'
        ),
        bill_docs as (
            select
                a.bill_id::int,
                d.doc_id::int,
                d.type as version_type,
                d.doc,
                to_char(doc_date, 'YYYY-MM-DD') as doc_date 
            from bill_meta a left join clean.bills_docs2 d on a.bill_id=d.bill_id::int
        ),
        sponsor_party as (
            select 
                bill_id,
                case when count(distinct party_id) > 1 then 'Bipartisan' else max(party_name) end as sponsor_party
                from bill_meta left join clean.bill_sponsors using(bill_id) join catalogs.political_party using(party_id)
            group by 1
        ),
        bill_status as (
            select 
                distinct on (bill_id) bill_id, status, to_char(progress_date, 'YYYY-MM-DD') as progress_date
            from bill_meta left join clean.bill_progress p using(bill_id) join catalogs.bill_status st on p."event"=st.status_id 
            where progress_date < '2020-06-01'
            order by bill_id, progress_date desc 		
        ),
        passage_predictions as (
            select	
                bill_id,
                case 
                    when score_long_term >= 0.80 then 'highly likely'
                    when (score_long_term < 0.8) and (score_long_term >=0.40) then 'somewhat likely'
                    when score_long_term < 0.4 then 'not likely'
                    else 'score not available'
                end as latest_passage_likelihood,
                as_of_date as prediction_date
            from bill_meta left join (
                select 
                    distinct on (bill_id) bill_id,
                    to_char(as_of_date, 'YYYY-MM-DD') as as_of_date,
                    (case when label_timespan='365day' then score else 0 end) as score_long_term
                from deploy.temp_passage_scores 
                order by bill_id, as_of_date desc
            ) as t using(bill_id)
        ),
        issue_areas as (
            with reproductive_rights as (
                select 
                    doc_id::int, 
                    max(reproductive_rights) as reproductive_rights
                from labels_es.reproductive_rights
                group by doc_id
            ),
            immigrant_rights as (
                select 
                    doc_id::int, 
                    max(immigrant_rights) as immigrant_rights
                from labels_es.immigrant_rights
                group by doc_id
            ),
            racial_justice as (
                select 
                    doc_id, 
                    max(racial_justice) as racial_justice
                from labels_es.racial_justice
                group by doc_id
            )
            select 
                bill_id,
                doc_id,
                reproductive_rights,
                immigrant_rights,
                racial_justice
            from bill_docs 
                left join reproductive_rights using(doc_id) 
                    left join immigrant_rights using(doc_id) 
                        left join racial_justice using(doc_id) 
        )
        select 
            a.bill_id,
            bill_number,
            legiscan_link,
            state_link,
            bill_title,
            bill_description,
            introduced_date,
            session_id,
            session_title as session_name,
            state,
            state_abbreviation,
            latitude,
            longitude,
            coalesce(doc_id, -1) as doc_id,
            coalesce(version_type, 'none') as version_type, 
            coalesce(doc_date, '1969-01-01') as doc_date,
            doc,
            sponsor_party,
            progress_date,
            status as latest_status,
            latest_passage_likelihood,
            prediction_date as passage_prediction_date,
            coalesce(reproductive_rights, 0) as reproductive_rights,
            coalesce(immigrant_rights, 0) as immigrant_rights,
            coalesce(racial_justice, 0) as racial_justice
        from bill_meta a join sponsor_party using(bill_id) 
            join bill_docs using(bill_id) 
                join bill_status using(bill_id) 
                    join passage_predictions using (bill_id) 
                        join issue_areas using(doc_id)  
    """

    df = pd.read_sql(q, engine)

    return df.to_dict('record')

def _index_one_doc(es_connection, bill_scores, index):
    document_id = _create_es_doc_hash(
        bill_id=bill_scores['bill_id'], 
        doc_id=bill_scores['doc_id']
    )

    # issue areas
    temp = {k: bill_scores[k] for k in ['reproductive_rights', 'immigrant_rights', 'racial_justice']}
    issue_areas = _get_issues(temp)

    json_object = {
        "bill_id": bill_scores.get('bill_id'),
        "bill_number": bill_scores.get('bill_number'),
        "legiscan_link": bill_scores.get('legiscan_link'),
        "state_link": bill_scores.get('state_link'),
        "doc_id": bill_scores.get('doc_id'),
        "version_type": bill_scores.get('version_type'),
        "doc_date": bill_scores.get('doc_date'),
        "doc": bill_scores.get('doc'),
        "bill_title": bill_scores.get('bill_title'),
        "bill_description": bill_scores.get('bill_description'),
        "introduced_date": bill_scores.get('introduced_date'),
        "session_id": bill_scores.get('session_id'),
        "session_name": bill_scores.get('session_name'),
        "state": bill_scores.get('state_name'),
        "state_abbreviation": bill_scores.get('state_abbreviation'),
        "state_location": {
            "latitude": bill_scores.get("latitude"),
            "longitude": bill_scores.get("longitude")
        },
        "sponsor_party": bill_scores.get('sponsor_party'),
        "progress_date": bill_scores.get('progress_date'),
        "latest_status": bill_scores.get('latest_status'),
        "latest_passage_likelihood": bill_scores.get('latest_passage_likelihood'),
        "passage_prediction_date": bill_scores.get('passage_prediction_date'),
        "reproductive_rights_score": bill_scores.get('reproductive_rights'),
        "immigrant_rights_score": bill_scores.get('immigrant_rights'),
        "racial_justice_score": bill_scores.get('racial_justice'),
        "issue_areas": issue_areas
    }

    logging.info(json_object)
    es_connection.index(
        index=index, 
        id=document_id, 
        body=json.dumps(json_object)
    )


def run_pipeline():
    creds_folder = '../../conf/local/'
    creds_file = os.path.join(creds_folder, 'credentials.yaml')

    # connections
    es = get_elasticsearch_conn(creds_file)
    db_conn = get_db_conn(creds_file)

    index_name = 'bill_search_test'

    logging.info('Fetching predictions from the DB')

    predictions = _fetch_details_from_db(
        engine=db_conn
    )

    logging.info('Writing passage scores to elasticsearch index {}'.format(index_name))

    for pred in predictions:
        logging.info('Writing results of bill {} with doc {} to elasticsearch'.format(pred['bill_id'], pred['doc_id']))
        _index_one_doc(es_connection=es, bill_scores=pred, index=index_name)

if __name__ == '__main__':
    # Logger
    timestr = datetime.now().strftime("%y%m%d%H%M%S")   
    # logging.basicConfig(level=logging.INFO, filename=f"../../logs/results_to_es_{timestr}.DEBUG", filemode='w')
    logging.basicConfig(level=logging.INFO, filename=f"../../logs/search_idex_injest.DEBUG", filemode='w')
    logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

    run_pipeline()