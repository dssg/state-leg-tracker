from email import header
from logging import DEBUG
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go


from src.utils.general import get_elasticsearch_conn, get_db_conn
from src.webapp.utils.utils import fetch_last_prediction_date


import plotly.io as pio

pio.templates.default = "simple_white"

es = get_elasticsearch_conn('../../conf/local/credentials.yaml')
db = get_db_conn('../../conf/local/credentials.yaml')


def show_bills_scored_latest_run(last_prediction_date):
    q = f'''
        select 
            bill_id,
            title as "Title",
            bill_number as "Bill Number", 
            state as "State", 
            score as "Passage Likelihood", 
            rank_pct*100 as "Percentile", 
            as_of_date::date as "Prediction Date"
        from deploy.passage_predictions 
        join clean.bills using(bill_id)
        --where as_of_date='{last_prediction_date}'
        order by as_of_date desc, score desc limit 1000
    '''

    display_cols = [
        "Bill Number", 
        "Title",
        "State", 
        "Passage Likelihood", 
        "Prediction Date",
    ]

    df = pd.read_sql(q, db)

    df["Bill Number"] = df.apply(lambda x: f'<a href="?bill_id={x["bill_id"]}" target="_blank" rel="noopener noreferrer"> {x["Bill Number"]} </a>', axis=1)  

    df = df[display_cols]
    df["Passage Likelihood"] = round(df["Passage Likelihood"] * 100, 2) 
    # values = [df[x] for x in display_cols]
    
    # fig = go.Figure(
    #     data=go.Table(
    #         header=dict(
    #             values=display_cols,
    #             align='center',
    #             fill_color='azure'
    #         ),
    #         cells=dict(
    #             values=values,
    #             align='center',
    #             fill_color='white'
    #         )
    #     )
    # )

    # fig = go.Figure(data=[go.Table(header=dict(values=list(df.columns)),
                #  cells=dict(values=df.values))])

    # fig.update_layout(width=800, height=1000)
    st.markdown(f'#### Bills Scored ')
    # st.plotly_chart(fig, use_container_width=True,  autosize=True)
    
    df = df[display_cols].to_html(escape=False, index=False)
    
    st.write(df, unsafe_allow_html=True)
    


def get_num_bills_scored(last_prediction_date):
    # q = f'''
    #     select 
    #         count(distinct bill_id) as num_bills
    #     from deploy.passage_predictions as p
    #     where as_of_date='{last_prediction_date}'
    # '''

    # q = f'''
    #     select 
    #         count(distinct bill_id) as num_bills
    #     from deploy.passage_predictions as p
    #     where as_of_date BETWEEN TODAY() - INTERVAL '1week' AND TODAY()
    # '''


    # pd.read_sql(q, db_conn)['num_bills'].iloc[0]

    q = {
        "query": {
            "match": {
            "as_of_date": last_prediction_date
            }
        }
    }

    num_bills = es.count(body=q, index='bill_scores_temp')['count']
    
    # st.markdown(f'#### Active Bills: {num_bills} ')

    return num_bills


def get_state_activity(last_prediction_date):
    q = {
        "query": {
            "match": {
                "as_of_date": last_prediction_date
            }
        }
    }

    q = {
        "size": 0, 
        "query": {
            "match": {
            "as_of_date": last_prediction_date
            }
        },
        "aggs": {
            "group_by_party": {
                "terms": {
                    "field": "state.keyword",
                    "size": 50
                }
            }
        }
    }


    state_counts = es.search(body=q, index='bill_scores_temp')['aggregations']['group_by_party']['buckets']
    df = pd.DataFrame(state_counts)
    num_states = int(len(df))

    if not df.empty:
        df['key'] = df['key'].str.capitalize()

    # st.markdown(f'#### Active States: {num_states}')

    return num_states, df


def plot_likelihood_dist(last_prediction_date):
    q = f'''
        select 
            bill_id, score
        from deploy.passage_predictions
        where as_of_date='{last_prediction_date}'
    '''

    df = pd.read_sql(q, db)

    if df.empty:
        return

    fig = px.histogram(df, x='score')
    st.markdown('#### Passage Score Distribution')
    st.plotly_chart(fig, use_container_width=True, width=200, height=600, autosize=False)


def plot_activity_by_party(last_prediction_date):
    # q = {
    #     "query": {
    #         "match": {
    #             "as_of_date": last_prediction_date
    #         }
    #     }
    # }

    q = {
        "size": 0, 
        "query": {
            "match": {
            "as_of_date": last_prediction_date
            }
        },
        "aggs": {
            "group_by_party": {
                "terms": {
                    "field": "sponsor_party.keyword"
                }
            }
        }
    }


    party_counts = es.search(body=q, index='bill_scores_temp')['aggregations']['group_by_party']['buckets']

    df = pd.DataFrame(party_counts)

    if df.empty:
        return

    fig = px.bar(        
        df,
        y = "key",
        x = "doc_count",
        labels={
            "key": "",
            "doc_count": ""
        },
        template='simple_white'
        # title = "Activity"
    )

    layout = go.Layout(
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)'
    )


    
    st.markdown('#### Activity by Party')
    st.plotly_chart(fig, use_container_width=True, width=300, height=300, autosize=True)


def plot_activity_by_state(state_counts):

    if state_counts.empty:
        return

    state_counts.sort_values('key', ascending=False, inplace=True)
    fig = px.bar(        
        state_counts,
        y = "key",
        x = "doc_count",
        labels={
            "key": "",
            "doc_count": ""
        }
        # title = "Activity by State"
    )

    st.markdown('#### Activity By State')
    st.plotly_chart(fig, use_container_width=True, width=200, height=600, autosize=False)


def plot_bills_by_passage_likelihood(last_prediction_date):
    q = {
        "size": 0, 
        "query": {
            "match": {
            "as_of_date": last_prediction_date
            }
        },
        "aggs": {
            "group_by_passage": {
                "terms": {
                    "field": "likelihood_long_term.keyword"
                }
            }
        }
    }

    passage_counts = es.search(body=q, index='bill_scores_temp')['aggregations']['group_by_passage']['buckets']



    df = pd.DataFrame(passage_counts)

    if df.empty:
        return

    fig = px.bar(        
        df,
        y = "key",
        x = "doc_count",
        labels={
            "key": "",
            "doc_count": ""
        }
        # title = "Activity"
    )
    st.markdown('#### Activity by Passage Likelihood')

    config = {'displayModeBar': True}
    st.plotly_chart(fig, use_container_width=True, width=100, height=100, autosize=False, config=config)



def plot_mean_passage_score_by_state(last_prediction_date):

    q = {
        "query": {
            "match": {
            "as_of_date": last_prediction_date
            }
        },
        "_source": ["state", "score_long_term", "bill_id"]
    }


    all_results = list()
    res = es.search(index='bill_scores_temp', body=q, scroll='1m', size=100, timeout='30s')
    hits = res['hits']['hits']
    scroll_id = res.get('_scroll_id')

    all_results = all_results + [x['_source'] for x in hits]

    stop = False
    while not stop:
        res = es.scroll(scroll_id=scroll_id, scroll='5m')
        hits = res['hits']['hits']
        scroll_id = res.get('_scroll_id')

        if len(hits) == 0:
            stop = True
            continue

        all_results = all_results + [x['_source'] for x in hits]

    df_all = pd.DataFrame(all_results)

    if df_all.empty:
        return

    grp_obj = df_all.groupby('state')

    grouped_res = list()
    for g, df in grp_obj:
        d = dict()
        d['state'] = g
        d['score'] = df['score_long_term'].mean()
        d['sd'] = df['score_long_term'].std()

        grouped_res.append(d)

    df = pd.DataFrame(grouped_res)
    df['state'] = df['state'].str.capitalize()
    df.sort_values('state', inplace=True, ascending=False)

    fig = go.Figure()

    fig.add_trace(go.Bar(
        name='',
        y=df['state'], x=df['score'],
        error_x=dict(type='data', array=df['sd']),
        orientation='h'
    ))

    st.markdown('#### Mean Passage Score by State')

    st.plotly_chart(fig, use_container_width=True, width=200, height=200, autosize=False)

    

def plot_mean_passage_score_by_state_and_party(last_prediction_date):

    q = {
        "query": {
            "match": {
            "as_of_date": last_prediction_date
            }
        },
        "_source": ["state", "score_long_term", "bill_id", "sponsor_party"]
    }

    all_results = list()
    res = es.search(index='bill_scores_temp', body=q, scroll='1m', size=100, timeout='30s')
    hits = res['hits']['hits']
    scroll_id = res.get('_scroll_id')

    all_results = all_results + [x['_source'] for x in hits]

    stop = False
    while not stop:
        res = es.scroll(scroll_id=scroll_id, scroll='1m')
        hits = res['hits']['hits']
        scroll_id = res.get('_scroll_id')

        if len(hits) == 0:
            stop = True
            continue

        all_results = all_results + [x['_source'] for x in hits]

    df = pd.DataFrame(all_results)

    if df.empty:
        return
    
    df['state'] = df['state'].str.capitalize()

    pivot = pd.pivot_table(df, values='score_long_term', index=['state'],
                    columns=['sponsor_party'], aggfunc=np.mean)

    fig = px.imshow(
        pivot,
        labels={
            "x": " ",
            "y": " "
        },
        aspect="auto",
        color_continuous_scale='Blues'
    )

    st.markdown('#### Mean Passage Score by Poltical Party & State')
    st.plotly_chart(fig, use_container_width=True, width=400, height=200, autosize=False)


def main():

    last_prediction_date = fetch_last_prediction_date(db)

    # last_prediction_date = '2021-01-30'

    num_bills = get_num_bills_scored(last_prediction_date)
    num_states, state_counts = get_state_activity(last_prediction_date)

    st.markdown('<center><h2> State Level Legislative Activity </h2></center>', unsafe_allow_html=True)

    st.markdown(f'<center> <h3> As of {last_prediction_date}, {num_bills} Bills are active across {num_states} States </h3> </center>', unsafe_allow_html=True)
    st.markdown('---')

    col1, col2, col3 = st.columns((1,1,1))

    with col1:
        plot_activity_by_state(state_counts)

        plot_likelihood_dist(last_prediction_date)

    with col2:
        plot_mean_passage_score_by_state(last_prediction_date)

        plot_bills_by_passage_likelihood(last_prediction_date)

        
    
    with col3:
        plot_mean_passage_score_by_state_and_party(last_prediction_date)

        plot_activity_by_party(last_prediction_date)

        

    show_bills_scored_latest_run(last_prediction_date)


    # col1, col3 = st.columns((2, 1.5))

    # with col1:
    #     show_num_bills_scored(last_prediction_date)

    #     state_counts = show_num_active_states(last_prediction_date)

    #     plot_likelihood_dist(last_prediction_date)

    # # with col2:
    #     plot_activity_by_party(last_prediction_date)

    #     plot_activity_by_state(state_counts)

    #     plot_bills_by_passage_likelihood(last_prediction_date)


    # # Rightmost column
    # with col3:
    #     show_bills_scored_latest_run(last_prediction_date)


    