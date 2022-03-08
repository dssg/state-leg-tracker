from pydoc import doc
from numpy import source
import pandas as pd
import streamlit as st
from streamlit.state.session_state import SessionState
import base64
import plotly.express as px

from src.utils.general import get_elasticsearch_conn, get_db_conn
from src.utils.project_constants import BILL_TEXT_INDEX
from src.webapp.utils.utils import get_states

from src.utils.decoders import html_decoder

es = get_elasticsearch_conn('../../conf/local/credentials.yaml')
db = get_db_conn('../../conf/local/credentials.yaml')


def get_latest_doc(db_conn, bill_id):
    q = f'''
        select distinct on (bill_id)
            doc_id
        from clean.bill_docs where bill_id={bill_id}
        order by bill_id, doc_date desc
    '''

    # TODO Handle errors
    df = pd.read_sql(q, db_conn)
    
    if df.empty:
        return None
    
    d = df.to_dict('records')[0]

    return d['doc_id']


def show_bill_text(doc_id):

    st.write('#### Latest Text')
    # fetch PDF
    query = {
        "query":{
            "match": {
                "doc_id": doc_id
            }
        }
    }

    return_fields = [
        'bill_id',
        'doc_id',
        'doc_date',
        'type',
        'type_id',
        'mime',
        'mime_id',
        'text_size',
        'bill_id',
        'doc',
        'encoded_doc'
    ]
    res = es.search(index=BILL_TEXT_INDEX, body=query, _source_includes=return_fields)
    
    if res.get('hits').get('hits'):
        res = res.get('hits').get('hits')[0]['_source']
        # st.write(res.keys())
    #   st.write(res['mime_id'])
        # encoded_doc = res.get('encoded_doc')

        # if not encoded_doc:
            # st.error('No document available!')
            # return 'Error: Document Not Available'
    else:
        st.error('No document available!')
        return 'Error: Document Not Available'

    encoded_doc = res.get('encoded_doc')

    decoded_text = res.get('doc')

    if res['mime_id'] == 1:
        # disp_html = {}

        if not decoded_text:
            decoded_text = base64.b64decode(encoded_doc, validate=True)
       
        
        # if encoded_doc:
        #     decoded_text = base64.b64decode(res['encoded_doc'], validate=True)
        # else:
        #     decoded_text = res.get('doc')
        
        # # st.write(res.get('doc'))
        
        try:
            # st.write(decoded_text)
            disp_html = decoded_text.decode('utf-8')
            if '<table' in disp_html:
                disp_html = html_decoder(decoded_text) 
                st.warning("_Warning: HTML Parsing can be suboptimal_")
                st.markdown(disp_html, unsafe_allow_html=True)
        except:
            try:
                disp_html = html_decoder(decoded_text) 
                st.markdown(res.get('doc'), unsafe_allow_html=True)
            except:
                disp_html = 'Error: Invalid HTML'
                st.error(disp_html)

    elif res['mime_id'] == 2: 
        disp_html = ''
        try:
            disp_html = f'''
                <embed src="data:application/pdf;base64,{res['encoded_doc']}" 
                width="95%" height="1000" type="application/pdf">
            '''
            st.markdown(disp_html, unsafe_allow_html=True)
        except Exception as e:
            print(res.keys())
            if decoded_text:
                st.warning('Formatted PDF not available!')
                disp_html = decoded_text
                st.write(decoded_text)
            else:
                disp_html = 'Something broke! No valid bill text'
                st.error(disp_html)

        

    else:
        disp_html = 'Error: Invalid MIME type'
        st.error(disp_html)

    return disp_html


def show_page_title(conn, bill_id):
    q = f'''
        select
            bill_id,
            bill_number,
            state,
            introduced_date,
            extract(year from introduced_date)::int as intro_year,
            session_title
        from clean.bills join clean.sessions using(session_id)
        where bill_id={bill_id}
    '''

    # TODO: Handle errors
    df = pd.read_sql(q, conn)
    
    if df.empty:
        st.error('Error: Bill details not available')
        return None
    d = df.to_dict('records')[0]
    st.markdown(
        f" <h2> <center> {d['bill_number']} | {d['state']} | {d['intro_year']} | {d['session_title']} </h2> ",
        unsafe_allow_html=True
    )


def plot_bill_scores(db_conn, bill_id):
    q = f'''
        select 
            as_of_date, score
        from deploy.passage_predictions
        where bill_id={bill_id}
        order by as_of_date
    '''
    chart_data = pd.read_sql(q, db_conn)
    st.write('#### Passage Prediction History')
    if chart_data.empty:
        st.warning('No prediction history available')
        return 
    
    fig = px.line(
        chart_data, 
        x="as_of_date", y="score", 
        template='seaborn',
        markers=True
    )

    fig.update_xaxes(title_text='Time')
    fig.update_yaxes(title_text='Passage Prediction', range = [0,1])
    
    st.plotly_chart(fig, use_container_width=True)
    # st.line_chart(chart_data.set_index('as_of_date'), use_container_width=True)


def show_bill_sponsors(db_conn, bill_id):
    q = f"""
        select 
            name, 
            pp.party_name as "Affiliation", 
            CASE WHEN r.role_id = 1 THEN 'House'
            WHEN r.role_id = 2 THEN 'Senate'
            ELSE 'Joint Ch.' END as "Chamber", 
            sponsor_start_date as "Sponsored Date"
        from clean.bill_sponsors a 
        join clean.session_people b on a.sponsor_id=b.people_id
        join catalogs.roles r using(role_id)
        join catalogs.political_party pp on a.party_id=pp.party_id
        where bill_id={bill_id}
        group by 1, 2, 3, 4
    """

    df = pd.read_sql(q, db_conn)
    st.write('#### Bill Sponsors')
    st.dataframe(df.set_index('name'), height=500)


def show_event_history(db_conn, bill_id):
    q = f"""
        select 
            event_date, chamber as "Chamber", INITCAP(action) as "Action"
        from clean.bill_events
        where important = 1
        and bill_id={bill_id}
        order by event_date desc
    """
    df = pd.read_sql(q, db_conn)
    
    st.write('#### Event History')
    
    if df.empty:
        st.warning('No event history available')
        return 
    st.dataframe(df.set_index('event_date'), height=600, width=700)


def show_voting_history(db_conn, bill_id):

    st.write('#### Votes')

    q = f'''
        select 
            vote_date, 
            concat('yea - ', yea, ', nay - ', nay, ', nv - ', nv, ', absent - ', absent) as "Result", 
            case 
                when passed then 'Yes'
                when not passed then 'No'
                else null 
            end as "Passed?",
            description
        from clean.bill_votes 
        where bill_id = {bill_id}
        order by vote_date desc
    '''

    df = pd.read_sql(q, db_conn)

    if df.empty:
        st.warning('No voting information available')
    else:
        st.dataframe(df.set_index('vote_date'))


def show_summary(db_conn, bill_id):
    q = f"""
        select 
            name, pp.party_name, r.role_name, sponsor_start_date
        from clean.bill_sponsors a 
        join clean.session_people b on a.sponsor_id=b.people_id
        join catalogs.roles r using(role_id)
        join catalogs.political_party pp on a.party_id=pp.party_id
        where bill_id={bill_id}
        group by 1, 2, 3, 4
    """

    df = pd.read_sql(q, db_conn)
    party_counts = df['party_name'].value_counts().to_dict()

    # TODO -- Do this BETTER!
    num_parties = len(party_counts)
    if num_parties > 1:
        spectrum_str = 'Bipartisan ('
        for i, p in enumerate(party_counts.keys()):
            v = party_counts[p]
            if i < num_parties - 1:
                spectrum_str += f' {p}-{v},'
            else: 
                spectrum_str += f' {p}-{v}'
        spectrum_str += ')'
    elif num_parties == 0:
        spectrum_str = 'None'
    else:
        spectrum_str = df['party_name'].iloc[0] 
    

    q = f'''
        select distinct on (bill_id)
            bill_id,
            p."event",
            p.progress_date,
            s.status
        from  clean.bill_progress p join catalogs.bill_status s on p."event"=s."status_id" and p."event" <= 6
        where bill_id={bill_id}
        order by bill_id, progress_date desc
    '''

    df = pd.read_sql(q, db_conn)

    status = f'''{df['status'].iloc[0]} on {df['progress_date'].iloc[0]}'''



    st.write('#### Current Status')
    st.markdown(f'''

        <table style="border:none">
            <tr>
                <td> <b/>Status </td>
                <td> {status} </td>
            </tr>
            <tr>
                <td> <b/>Affiliation </td>
                <td> {spectrum_str} </td>
            </tr>
        </table>
    ''',
    unsafe_allow_html=True
    )
    # st.write(f'**Spectrum:** </b> {spectrum_str} ')


def main(bill_id):

    if not bill_id:
        st.write('No Valid Bill ID set')
        return 

    show_page_title(db, bill_id)
    st.write(' ')
    col1, col2 = st.columns((1.3,1))

    with col1:
        show_summary(db, bill_id)
        st.write(' ')
        # PDF display
        doc_id = get_latest_doc(db, bill_id)
        if doc_id is not None:
            disp_string = show_bill_text(doc_id)
            
            # try:
            #     st.markdown(disp_string, unsafe_allow_html=True)
            # except:
            #     st.error('Error in rendering the document!')
        else:
            st.error('No document available!')

    with col2:
        plot_bill_scores(db, bill_id)
        show_bill_sponsors(db, bill_id)
        show_event_history(db, bill_id)
        show_voting_history(db, bill_id)

    
    
    






