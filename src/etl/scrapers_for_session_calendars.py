from pandas.core.indexes.base import Index
import requests
from datetime import datetime
import pandas as pd
import logging
import psycopg2
from bs4 import BeautifulSoup

from src.utils.general import get_db_conn

# FOr sessions until 2020, we obtained session convene/adjourn dates from NCSL via email. THis script can automate the process by scraping multistate

# NOTE: In this script we are only interested in regular sessions

# We should be able to just scrape multistate for session dates going forward
def multistate_scraper(session_year):
    """ Scraping multistate.us for session convene and adjourn dates 
        NOTE -- Multistate only contains 2021 & 2022 dates as of now 
        Args:
            session_year: year we are interested in
        return:
            A dataframe with three columns (state, convene_date, adjourn_date)
    """
    url = 'https://www.multistate.us/resources/{}-legislative-session-dates'.format(session_year)

    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'lxml')


    table = soup.find_all('table')[0]

    state_dates = list()
    for i, row in enumerate(table.find_all('tr')):
        # State, convene, adjournment
        if i > 1: # The first two rows are the headers
            cols = row.find_all('td')
            num_cols = len(cols)
            logging.info('Number of columns in the row {}'.format(num_cols))
            d = dict()

            # we only need the first three columns
            d['state'] = cols[0].get_text().lower()
            try:
                d['convene_date'] = datetime.strptime(cols[1].get_text(), '%m/%d/%y').date()
            except ValueError as e:
                logging.warning( 'No convene date for {} -- {}'.format(d['state'], cols[1].get_text()))
                d['convene_date'] = None
                d['adjourn_date'] = None
                continue

            try: 
                d['adjourn_date'] = datetime.strptime(cols[2].get_text(), '%m/%d/%y').date()
            except (ValueError, IndexError) as e:
                logging.warning( 'No adjourn date for {} -- {}'.format(d['state'], e))
                d['adjourn_date'] = None
            
            state_dates.append(d)

    df = pd.DataFrame(state_dates)
    df['session_year'] = session_year

    return df

def write_to_session_dates_table(engine, session_dates, session_year):
    # As we delete the existing records for that year from the DB before we add them, all the sessions should be in the dataframe as a prelim requirement
    if (session_year//2 ==0) and (len(session_dates) < 46): 
        # At least the dataframe should consist 46 states because in even years, 4 states do not hold regular sessions
        logging.warning('The dataframe does not contain 46 entried only contains {}'.format(len(session_dates)))
        # raise ValueError('The passed dataframe contains only {} entries. But should contain 46 entries (all states except TX, ND, NV, MT)')

    if (session_year //2 != 0 ) and (len(session_dates) < 50):
        logging.warning('The dataframe does not contain 50 entried only contains {}'.format(len(session_dates)))
        # raise ValueError('The passed dataframe contains only {} entries. But should contain 50 entries'.format(len(session_dates)))
   
    # checking whether records exist in the table for the year. Removing them to replace with the new ones
    logging.info('The dataframe contains {} entries'.format(len(session_dates)))

    q = """
        delete from raw.session_dates 
        where session_year={}
    """.format(session_year)

    cursor = engine.cursor()
    try:
        cursor.execute(q)
        engine.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        raise 

    var = [
        (
            x['session_year'],
            x['state'],
            x['convene_date'],
            x['adjourn_date'],
            False
        )
        for x in session_dates.to_dict('records')
    ]
    
    q = """insert into raw.session_dates 
            (session_year, state_name, convene_date, adjourn_date, special) 
        values (%s, %s, %s, %s, %s)"""

    cursor = engine.cursor()
    try:
        cursor.executemany(q, var)
        engine.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        raise 


if __name__ == '__main__':
    db_conn = get_db_conn('../../conf/local/credentials.yaml')
    year = 2021
    df = multistate_scraper(year)

    write_to_session_dates_table(
        engine=db_conn,
        session_dates=df,
        session_year=year
    )

    print(df)
    