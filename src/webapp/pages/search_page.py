import streamlit as st
from streamlit.state.session_state import SessionState
from src.webapp.utils.search_utils import index_search
from src.webapp import templates
import urllib.parse
import datetime

from src.utils.general import get_elasticsearch_conn, get_db_conn
from src.webapp.utils.utils import get_states

es = get_elasticsearch_conn('../../conf/local/credentials.yaml')
db = get_db_conn('../../conf/local/credentials.yaml')

PAGE_SIZE = 10


def set_session_state():
    # set default values
    if 'search' not in st.session_state:
        st.session_state.search = None
    # if 'tags' not in st.session_state:
    #     st.session_state.tags = None
    if 'page' not in st.session_state:
        st.session_state.page = 1

    # get parameters in url
    para = st.experimental_get_query_params()
    if 'search' in para:
        st.experimental_set_query_params()
        # decode url
        new_search = urllib.parse.unquote(para['search'][0])
        st.session_state.search = new_search
    if 'page' in para:
        st.experimental_set_query_params()
        st.session_state.page = int(para['page'][0])


def sidebar():
    states = get_states(db)
    states['All'] = 'All'
    state_leg = st.sidebar.selectbox(
        "Select the State Legislature:",
        ('All',) + tuple(states.keys())
    )

    passage = st.sidebar.selectbox(
        'Bills with Passage Likelihood',
        ['All', 'Highly Likely', 'Likely', 'Somewhat Likely', 'Unlikely', 'Highly Unlikely']
    )

    activity_threshold = st.sidebar.date_input(
        "Search Bills with any movement after:",
        value=datetime.date(2021, 1, 1)
    )

    only_active = st.sidebar.checkbox(
        'Only Search Active Bills', True
    )

    only_latest = st.sidebar.checkbox(
        'Only Search Latest Version of Bill', True
    )

    only_curr_sess = st.sidebar.checkbox(
        'Only Search the Latest Session', True
    )
    

    d = {
        'state_leg': states[state_leg],
        'passage': passage,
        'search_only_active': only_active,
        'search_only_latest': only_latest,
        'search_only_active_session': only_curr_sess,
        'activity_threshold': activity_threshold.strftime("%Y-%m-%d")
    }

    return d


def main():
    set_session_state()
    filters = sidebar()
    st.title('Search Legislative Bills')

    if st.session_state.search is None:
        search = st.text_input('')
    else:
        search = st.text_input('', st.session_state.search)

    
    if search:
        from_i = (st.session_state.page - 1) * PAGE_SIZE
        results = index_search(
            es=es, 
            db_conn=db,
            index='bill_meta', 
            keywords=search, 
            from_i=from_i, 
            size=PAGE_SIZE,
            filters=filters)
        total_hits = results['hits']['total']['value']

        # show number of results and time taken
        st.write(templates.number_of_results(total_hits, results['took'] / 1000),
                 unsafe_allow_html=True)

        # if total_hits > PAGE_SIZE:
        #     total_pages = (total_hits + PAGE_SIZE - 1) // PAGE_SIZE
        #     pagination_html = templates.pagination(total_pages, search,
        #                                            st.session_state.page)
        #     st.write(pagination_html, unsafe_allow_html=True)

        # # render popular tags as filters
        # # st.write(templates.tag_boxes(search_term, results['sorted_tags'][:10], ''),
        # #          unsafe_allow_html=True)
        # search results
        for i in range(len(results['hits']['hits'])):
            result = results['hits']['hits'][i]
            res = result['_source']
            res['session_name'] = res['session']['session_name']
            # res['filters'] = {'date_lb': filters['last_intro_date']}
            
            # res['highlights'] = '...'.join(result['highlight']['description'])
            st.write(templates.search_result(i + from_i, **res), unsafe_allow_html=True)
            # st.write(templates.tag_boxes(search_term, res['tags'], ''),
            #          unsafe_allow_html=True)

        # pagination
        if total_hits > PAGE_SIZE:
            total_pages = (total_hits + PAGE_SIZE - 1) // PAGE_SIZE
            pagination_html = templates.pagination(total_pages, search,
                                                   st.session_state.page)
            st.write(pagination_html, unsafe_allow_html=True)

if __name__ == '__main__':
    main()