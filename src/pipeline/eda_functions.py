import logging
import itertools
import pandas as pd
import matplotlib.pyplot as plt
import logging

from wordcloud import WordCloud, STOPWORDS  


def setup_figure(fig_size=(12,4)):
    """Setup the figure and Axes object"""
    fig, ax = plt.subplots()
    fig.set_size_inches(fig_size)
    ax.spines['right'].set_visible(False)
    ax.spines['top'].set_visible(False)
    
    return fig, ax

def get_bills(db_con):
    """ fetch all bills and reprductive bills from postgress"""

    q = """
        select 
            bill_id, 
            max(doc_date) as last_ver_date, 
            min(doc_date) as first_ver_date
            from temp_eda.repro_labels_all 
        group by bill_id;
        """

    # repro bills
    repro_bills = pd.read_sql(q, db_con)

    # TODO: This is hacky, the column format is numeric, fix it
    repro_bills['bill_id'] = repro_bills['bill_id'].astype(int).astype(str)

    # all bills
    q = """select * from temp_eda.bills_metadata;"""
    all_bills = pd.read_sql(q, db_con)

    # The long state names
    q = """ select * from clean.states"""
    all_states = pd.read_sql(q, db_con)

    all_bills = all_bills.merge(all_states, left_on='state', right_on='state_abbreviation', how='left')
    
    all_bills.drop('state_x', axis=1, inplace=True)
    all_bills.rename(columns={'state_y': 'state'}, inplace=True)

    return all_bills, repro_bills


def get_bill_versions(db_con):
    q = """
        select 
            bill_id, 
            count(*) as num_versions, 
            min(doc_date) as intro_date, 
            max(doc_date) as last_date 
        from temp_eda.bill_docs group by bill_id"""

    all_bill_versions = pd.read_sql(q, db_con)
    all_bill_versions['bill_id'] = all_bill_versions['bill_id'].astype(int).astype(str)

    q = """
        select 
            bill_id, 
            count(*) as num_versions, 
            min(doc_date) as intro_date, 
            max(doc_date) as last_date 
        from temp_eda.repro_labels_all group by bill_id"""
    
    repro_bill_versions = pd.read_sql(q, db_con)
    repro_bill_versions['bill_id'] = repro_bill_versions['bill_id'].astype(int).astype(str)

    return all_bill_versions, repro_bill_versions


def get_repro_bill_fraction_state(all_bills, repro_bills):
    """Get the fraction of reproductive bills in each state"""
    
    repro_bills = repro_bills.merge(all_bills, on='bill_id', how='left')

    # Country level fraction
    repro_bill_fraction = repro_bills.shape[0]/all_bills.shape[0]
    logging.info('Fraction of reproduction bills, overall: {:.3f}%'.format(repro_bill_fraction*100))

    # Reproductive bills by state
    repro_by_state = repro_bills.groupby('state', as_index=False).count()[['state', 'bill_id']]
    repro_by_state = repro_by_state.rename(columns={'bill_id': 'bill_count'})

    # all bills by state
    by_state = all_bills.groupby('state', as_index=False).count()[['state', 'bill_id']]
    by_state = by_state.rename(columns={'bill_id': 'bill_count'})

    # Calculating the fraction
    t1 = repro_by_state.set_index('state')
    t2 = by_state.set_index('state')

    repro_frac_state = pd.DataFrame()
    repro_frac_state['repro_frac'] = t1['bill_count']/t2['bill_count']
    repro_frac_state.reset_index(inplace=True)
    repro_frac_state.sort_values(by='repro_frac', ascending=False, inplace=True)

    return repro_frac_state


def get_repro_bill_frac_year(all_bills, repro_bills):
    """Get the fraction and bill counts per state for last 5 years"""

    repro_bills = repro_bills.merge(all_bills, on='bill_id', how='left')

    # Grouping by state and year
    repro_yearly_counts = repro_bills.groupby(
        ['session_year_start', 'state'], 
        as_index=False
        ).count()[['session_year_start', 'bill_id', 'state']]
    repro_yearly_counts.rename(columns={'bill_id': 'bill_count'}, inplace=True)

    # All yearly counts
    all_yearly_counts = all_bills.groupby(
        ['session_year_start', 'state'], 
        as_index=False
    ).count()[['session_year_start', 'bill_id', 'state']]
    all_yearly_counts = all_yearly_counts.rename(columns={'bill_id': 'bill_count_all'})

    # All sates and years combinations
    states = repro_yearly_counts['state'].unique()
    years = [2015, 2016, 2017, 2018, 2019]
    states_years = [{'state': s, 'year': y} for s, y in itertools.product(states, years)]
    states_years = pd.DataFrame(states_years)

    # Repro
    repro_yearly_counts = states_years.merge(
        repro_yearly_counts, 
        left_on=['state', 'year'], 
        right_on=['state', 'session_year_start'], 
        how='left'
    )[['state', 'year', 'bill_count']]
    repro_yearly_counts = repro_yearly_counts.fillna(0)

    # All
    all_yearly_counts = states_years.merge(
        all_yearly_counts, 
        left_on=['state', 'year'], 
        right_on=['state', 'session_year_start'], how='left'
    )[['state', 'year', 'bill_count_all']]
    all_yearly_counts = all_yearly_counts.fillna(0)

    combined = repro_yearly_counts.merge(all_yearly_counts, on=['state', 'year'])
    combined.set_index(['state', 'year'], inplace=True)

    combined['fraction'] = combined['bill_count'] / combined['bill_count_all']
    combined.reset_index(inplace=True)

    return combined


def get_repro_bill_texts(es, db_con):
    """Fetching all bill texts for the repro bills"""

    q = """ select doc_id from temp_eda.repro_labels_all;"""

    repro_docids = pd.read_sql(q, db_con)
    repro_docids = [int(x) for x in repro_docids['doc_id'].tolist()]

    bill_texts=pd.DataFrame()

    for doc_id in repro_docids:
        d = dict()
        d['doc_id'] = int(doc_id)
        query = {'query': {'match': {'doc_id': doc_id}}}
        res = es.search(index='bill_text', body=query, size=100)

        d['doc'] = res['hits']['hits'][0]['_source']['doc']
        d['doc_date'] = res['hits']['hits'][0]['_source']['doc_date']
        d['bill_id'] = res['hits']['hits'][0]['_source']['bill_id']

        bill_texts = bill_texts.append(d, ignore_index=True) 


    bill_texts['doc_id'] = bill_texts['doc_id'].astype('int').astype('str')
    bill_texts['bill_id'] = bill_texts['bill_id'].astype('int').astype('str')

    return bill_texts


def _preprocess_text_for_cloud(bill_texts):
    """ preprocessing text for word could"""

    # Converting to lower case
    preprocessed = bill_texts['doc'].str.lower()

    return preprocessed


def _generate_word_list_from_docs(docs_list):
    """ Tokenize a list of documents to a single string of words"""
    words = ''
    for doc in docs_list:
        text = str(doc)
        tokens = text.split()
        
        words += ", ".join(tokens)+", "

    return words


def get_wordcloud(docs_list):
    """ given a list of documents, get a word could"""
    words_list = _generate_word_list_from_docs(docs_list)
    stopwords = set(STOPWORDS)

    del docs_list

    wordcloud = WordCloud(width = 800, height = 800, 
            background_color ='white', 
            stopwords = stopwords, 
            min_font_size = 10, max_words=1000).generate(words_list) 

    return wordcloud

def _temp_get_words_list(es, db_con, repro_texts=None):
    if repro_texts is None:
        repro_texts = get_repro_bill_texts(es, db_con)

    # Only keeping the last version of the bill
    logging.info('Extracting the last version of the bill')
    last_doc_date = repro_texts.groupby('bill_id', as_index=False)['doc_date'].max()
    repro_texts = last_doc_date.merge(repro_texts, on=['bill_id', 'doc_date'], how='left')

    # repro_texts_last = repro_texts.groupby('bill_id', as_index=False).filter(lambda s: s['doc_date']==s['doc_date'].max())

    logging.info('Preprocessing ')
    # repro_texts_last['preprocessed_text'] = _preprocess_text_for_cloud(repro_texts_last)
    repro_texts['doc'] = repro_texts['doc'].str.lower()
    
    doc_list = repro_texts['doc'].tolist()

    logging.info('releasing repro')
    del repro_texts
    
    stopwords = set(STOPWORDS)
    words = ''
    logging.info('creating word list')
    for doc in doc_list:
        # text = str(doc)
        tokens = doc.split()
        
        words += ", ".join(tokens)+", "

    return words




def repro_word_cloud_country(es, db_con, repro_texts=None):
    """ Get a word could of the repro bills at country level """
    
    # if repro_texts is None:
    #     repro_texts = get_repro_bill_texts(es, db_con)

    # # Only keeping the last version of the bill
    # logging.info('Extracting the last version of the bill')
    # last_doc_date = repro_texts.groupby('bill_id', as_index=False)['doc_date'].max()
    # repro_texts = last_doc_date.merge(repro_texts, on=['bill_id', 'doc_date'], how='left')

    # # repro_texts_last = repro_texts.groupby('bill_id', as_index=False).filter(lambda s: s['doc_date']==s['doc_date'].max())

    # logging.info('Preprocessing ')
    # # repro_texts_last['preprocessed_text'] = _preprocess_text_for_cloud(repro_texts_last)
    # repro_texts['doc'] = repro_texts['doc'].str.lower()
    
    # doc_list = repro_texts['doc'].tolist()

    # logging.info('releasing repro')
    # del repro_texts
    
    
    # words = ''
    # logging.info('creating word list')
    # for doc in doc_list:
    #     # text = str(doc)
    #     tokens = doc.split()
        
    #     words += ", ".join(tokens)+", "

    # del doc_list
    words = _temp_get_words_list(es, db_con, repro_texts=repro_texts)
    stopwords = set(STOPWORDS)
    logging.info('creating word cloud')
    
    wordcloud = WordCloud(width = 800, height = 800, 
            background_color ='white', 
            stopwords = stopwords, 
            min_font_size = 10, max_words=500).generate(words)

    return wordcloud
    


def get_states_word_clouds_reproductive_rights(es, db_con):
    """ generate word clouds for each state """

    q = "select * from temp_eda.bill_docs;"
    all_bill_docs = pd.read_sql(q, db_con)
    all_bill_docs['doc_id'] = all_bill_docs['doc_id'].astype('int').astype('str')

    # TODO: Remove, Reading in from the csv file for testing    
    repro_texts = pd.read_csv('../../data/repro_bill_texts.csv')

    # fetch texts for reproductive bills
    # repro_texts = get_repro_bill_texts(es, db_con)

    # repro_texts = get_repro_bill_texts(es, db_con)
    all_bills, _ = get_bills(db_con)

    logging.info('Extracting the last version of the bill')
    last_doc_date = repro_texts.groupby('bill_id', as_index=False)['doc_date'].max()
    repro_texts_last = last_doc_date.merge(repro_texts, on=['bill_id', 'doc_date'], how='left')

    repro_texts_last['preprocessed_text'] = _preprocess_text_for_cloud(repro_texts_last)

    # Getting state_details
    repro_texts_last_details = repro_texts_last.merge(all_bills, on='bill_id', how='left')

    state_wordclouds = repro_texts_last_details.groupby('state', as_index=False).apply(lambda x: get_wordcloud(x['preprocessed_text'].tolist()))


    return state_wordclouds










    


    

    