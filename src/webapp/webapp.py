import streamlit as st
from src.webapp.pages import search_page, bill_page, dashboard

class WebApp:
    """"""

    def __init__(self) -> None:
        self.search_page = search_page
        self.bill_page = bill_page
        self.dashboard = dashboard

    def add_page(self, title, func) -> None:

        self.pages.append({
                "title": title, 
                "function": func
        })
    
    def get_url_params(self):
        para = st.experimental_get_query_params()

        return para


    def run(self):
        st.set_page_config(layout="wide")
        view = st.sidebar.radio(
            label='Current View',
            options=('Text Search', 'Model Dashboard'),
            index=0
        )

        param = self.get_url_params()

        # The bill page is loaded if the query parameter 'bill_id' is set
        if 'bill_id' in param:
            self.bill_page.main(param['bill_id'][0])

        else:
            if view == 'Text Search':
                self.search_page.main()

            elif view == 'Model Dashboard':
                self.dashboard.main()


