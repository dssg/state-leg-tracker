import requests
import pandas as pd
from bs4 import BeautifulSoup

from src.utils.general import get_db_conn


def get_summary_stats_all_states(session_year):
    """ Scrape legican's state landing page for the given years bill intro summary
    """

    q = """
        select
            state_abbreviation as state
        from catalogs.states
    """
    db_conn = get_db_conn('../../conf/local/credentials.yaml')

    states = pd.read_sql(q, db_conn)['state'].tolist()

    summaries = list()
    for state in states:
        d = scrape_regular_session_summary(state, session_year)
        d['state'] = state

        summaries.append(d)

    return summaries


def scrape_regular_session_summary(state, session_year):
    """ Scraping legiscan for summary stats for each state (only regular sessions). 
        This is to check whether our DB matches the LegiScan data

        Args:
            state (str): The abbreviate name of the state that is being checked
            session_year (int): The year of the session

        Return:
            a dictionary with number of introduced bills, and completed bills
    """

    url = 'https://www.legiscan.com/{state_abbreviation}'.format(
        state_abbreviation=state
    )

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')

    # upon inspecting, found that the last table is index 7
    # table columns -- Year, session, introduced, completed, <legislation, search, dataset>
    table = soup.find_all('table')[7]

    for i, row in enumerate(table.find_all('tr')):
        if i > 0:
            cols = row.find_all('td')

            d = dict()   
            d['year'] = cols[0].get_text()
            d['session'] = cols[1].get_text()
            d['introduced'] = int(cols[2].get_text())
            d['completed'] = int(cols[3].get_text())

            # filter the row for the regular session of the year concerned
            if (str(session_year) in d['year']) and ('special' not in d['session']):
                return d
        
if __name__ == '__main__':
    summaries = get_summary_stats_all_states(2021)

    print(summaries)