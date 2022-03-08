import pandas as pd
import logging

from elasticsearch import Elasticsearch

def _parse_search_results(res):
    """parse the results response received from elastic search query search"""
    
    # The number of total hits for the query
    num_total_hits = res['hits']['total']['value']

    # The list of bills returned in the query
    hits = res['hits']['hits']

    # The ID of the scroll for pagination of a larger result
    scroll_id = res.get('_scroll_id')

    return num_total_hits, hits, scroll_id

def _get_df_from_hits(hits):
    """ Extract the relevant info from the search hits"""
    append_info = [
        {
            'bill_id': d['_source']['bill_id'], 
            'doc_id': d['_source']['doc_id'],
            'relevance_score': d['_score'],
        }
        for d in hits
    ]

    temp = pd.DataFrame(append_info)

    return temp


def get_bills_in_issue_area(es, search_index, search_key, search_phrases, issue_area, query_size=100):
    """Get the document IDs that are related to a issue area
    
        args:
            es:
                elastic search connection object
            search_index:
                elastic search index to search in
            search_phrases: List
                The list of phrases that we should search in the index
            issue_area: String
                The ACLU issue area (the label) the relevant bills will be assigned
    """

    # The dictionary that holds the relevant bill documents. 
    # The key is the search phrase and values are bill lists
    df = pd.DataFrame()

    for phrase in search_phrases:
        query = {'query': {'match': {search_key: phrase}}}

        # First search
        res = es.search(index=search_index, body=query, scroll='1m', size=query_size)
        _, hits, scroll_id = _parse_search_results(res)

        temp = _get_df_from_hits(hits)
        temp['search_phrase'] = phrase
        df = df.append(temp, ignore_index=True)

        stop = False
        while not stop:
            res = es.scroll(scroll_id=scroll_id, scroll='1m')
            _, hits, scroll_id = _parse_search_results(res)
            
            if len(hits) == 0:
                stop = True
                continue
            
            temp = _get_df_from_hits(hits)
            temp['search_phrase'] = phrase
            df = df.append(temp, ignore_index=True)
    
    # Assigning label
    df[issue_area] = 1

    # Converting the ids to integers
    df['bill_id'] = df['bill_id'].astype(int)
    df['doc_id'] = df['doc_id'].astype(int)
         
    logging.info('Returning {} bills for issue area--{}'.format(df.shape[0], issue_area))
    logging.info(df.head())

    return df
