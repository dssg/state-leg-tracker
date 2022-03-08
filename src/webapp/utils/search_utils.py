from datetime import date
import pandas as pd
from sqlalchemy import false

# Using 'Today' as the date for filtering active bills.
TODAY = date.today()


def index_search(es, db_conn, index, keywords, from_i, size=10, filters={'last_intro_date': '2022-01-01'}):
    # search query
    # body = {
    #     'query': {
    #         'bool': {
    #             'must': [
    #                 {
    #                     'query_string': {
    #                         'query': keywords,
    #                         'fields': ['title'],
    #                         'default_operator': 'AND',
    #                     }
    #                 }
    #             ],
    #         }
    #     },
    #     'highlight': {
    #         'pre_tags': ['<b>'],
    #         'post_tags': ['</b>'],
    #         'fields': {'content': {}}
    #     },
    #     'from': from_i,
    #     'size': size,
    #     # 'aggs': {
    #     #     'tags': {
    #     #         'terms': {'field': 'tags'}
    #     #     },
    #     #     'match_count': {'value_count': {'field': '_id'}}
    #     # }
    # }

    # if filters is not None:
    #     body['query']['bool']['filter'] = {
    #         'terms': {
    #             'tags': [filters]
    #         }
    #     }
    # body = {
    #     'query': {
    #         'match': {
    #             'title': keywords
    #         }
    #     },
    #     'highlight': {
    #         'pre_tags': ['<b>'],
    #         'post_tags': ['</b>'],
    #         'fields': {'description': {}}
    #     },
    #     'from': from_i,
    #     'size': size
    #     # 'aggs': {
    #     #     'tags': {
    #     #         'terms': {'field': 'state'}
    #     #     },
    #     #     'match_count': {'value_count': {'field': '_id'}}
    #     # }
    # }

    print('filters -- ', filters)
    body = {
        "min_score": 0.1,
        "query": { 
            "bool": {
                "should": [
                    {
                    "match": 
                    {
                        "title": {
                            "query": keywords,
                            "fuzziness": 2,
                            # "auto_generate_synonyms_phrase_query": false
                        }
                    }
                    },
                    {
                    "match": 
                    {
                        "description": {
                            "query": keywords,
                            "fuzziness": 2,
                            # "auto_generate_synonyms_phrase_query": false
                        }
                    }
                    }
                ]
            }
         },
         "highlight": {
            "fields": {
                "description": {
                    "pre_tags": ["<strong>"],
                    "post_tags": ["</strong>"]
                }
            }
        },
        'from': from_i,
    }

    # Filtering based on the state
    if filters['state_leg'] != 'All':
        if not body['query']['bool'].get('filter'):
            body['query']['bool']['filter'] = [{"match": {"state": filters['state_leg']}}]
        else:
            body['query']['bool']['filter'].append({"match": {"state": filters['state_leg']}})


    # Filtering the bills with movement beyond the specified date
    if filters['activity_threshold']:
        if not body['query']['bool'].get('filter'):
            body['query']['bool']['filter'] = [{ "range": { "history.date": { "gte": filters['activity_threshold'] }}}]
        else:
            body['query']['bool']['filter'].append({ "range": { "history.date": { "gte": filters['activity_threshold'] }}})


    # filteirng only the active bills
    if filters['search_only_active']:
        # query for fetching the list of active bills --> cohort query for the last as_of_date
        q = '''
            with all_active_bills as (
                select 
                    bill_id
                from clean.bills a
                    join clean.sessions b using (session_id)
                        join pre_triage_features.ajusted_session_dates c using (session_id)
                            left join clean.bill_progress d using (bill_id)
                where 
                    extract(year from date '{as_of_date}')::int in (b.year_start, b.year_end) 
                    and 
                    extract (year from introduced_date)::int in (b.year_start, b.year_end)
                    and
                    introduced_date < '{as_of_date}'
                    and
                    not b.special	 
                    and 
                    (adjourn_date > '{as_of_date}' or adjourn_date is null)
                    and
                    convene_date < '{as_of_date}'
                    and 
                    progress_date < '{as_of_date}'
                group by bill_id
                having
                    max(case when event in (4, 5, 6) then progress_date end) is null
            )
            select
                bill_id
            from all_active_bills left join clean.bill_events using (bill_id)
            where event_date < '{as_of_date}'
            group by bill_id
            having 
                min(('{as_of_date}'::DATE - event_date::DATE)::int) < 60
            
        '''.format(
            as_of_date=TODAY
        )

        bill_ids = pd.read_sql(q, db_conn)['bill_id'].tolist()

        if not body['query']['bool'].get('filter'):
            body['query']['bool']['filter'] = [{"ids": {"values": bill_ids}}]
        else:
            body['query']['bool']['filter'].append({"ids": {"values": bill_ids}})



    res = es.search(index=index, body=body, size=size)
    # sort popular tags
    # sorted_tags = res['aggregations']['tags']['buckets']
    # sorted_tags = sorted(
    #     sorted_tags,
    #     key=lambda t: t['doc_count'], reverse=True
    # )
    # res['sorted_tags'] = [t['key'] for t in sorted_tags]
    
    return res
