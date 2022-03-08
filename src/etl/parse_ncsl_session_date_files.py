import os
import pandas as pd
import csv
import psycopg2
import logging

from datetime import datetime

from src.utils.general import get_db_conn

""" 
    NCSL has the exact session dates for each year in PDF form. Can obtain it in a docx form by writing to them. 
    The functions here assume that the table in the docx file is copied to a .csv file
"""

def _parse_date_col(date_str, year):
    if (date_str=='*') or date_str=='': # NCSL uses * when the legislature meets throughout the year
        # date_str = str(year)+'-'+'12-31' # TODO: Find the best course of action
        return None
    
    # removing the '.' in the str
    date_str_cleaned = date_str.replace('.', '')
    date_str_cleaned = date_str_cleaned.replace('*', '') 
    date_str_cleaned = date_str_cleaned.lstrip().rstrip()
    
    # convert to datestring
    date_formats = ['%Y %d-%b', '%Y %B %d', '%Y %B %-d', '%Y %b %d', '%Y %b %-d', '%Y %m %d', '%d-%b-%y', '%-d-%b-%y', '%Y %b-%d']

     # print(date_str_cleaned)
    for df in date_formats:
        try:
            date_obj = datetime.strptime(date_str_cleaned, df)
            formatted_date = date_obj.strftime('%Y-%m-%d')

            return formatted_date
        except:
            # add the year
            date_str_cleaned2 = str(year) + ' ' + date_str_cleaned
            try:
                date_obj = datetime.strptime(date_str_cleaned2, df)
                formatted_date = date_obj.strftime('%Y-%m-%d')

                return formatted_date
            except:
                formatted_date = None

    if not formatted_date:
        logging.error('Date {},{} should be in one of the formats: {}'.format(date_str, date_str_cleaned2, date_formats))

    return formatted_date   


def parse_regular_session_dates_csv(file_path, year):
    """ Takes the raw CSV file for a year and parse it to obtain the regular session convene and adjourn dates for each state
        Args:
            file_path: Path for the csv file (file created using the)
    
    """
    with open(file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=',')

        # First four columns relate to the regular sessions
        regular_sessions = list()
        
        for i, row in enumerate(csv_reader):
            if i < 2:
                continue
            d = dict()    
            if len(row[0])>0: # When there are multiple special sessions, the first four columns are empty
                d['state'] = row[0].lower().replace(u'\xa0', u'').lstrip().rstrip()
                d['convene'] = _parse_date_col(row[1], year)
                d['adjourn'] = _parse_date_col(row[2], year)
                d['comments'] = row[3]
                # replace(u'\xa0', u' ')
                
                regular_sessions.append(d)

                # print(d)
                # break
                
        return regular_sessions

    
def write_to_session_dates_table(engine, csv_file, year, session_type='regular'):

    if session_type == 'regular':
        session_dates = parse_regular_session_dates_csv(csv_file, year)
        var = [
        (
            year,
            x['state'],
            x['convene'],
            x['adjourn'],
            False,
            x['comments']
        )
        for x in session_dates
    ]
    else:
        var=[]
        # TODO
        return

    q = """insert into raw.session_dates 
            (session_year, state_name, convene_date, adjourn_date, special, notes) 
        values (%s, %s, %s, %s, %s, %s)"""

    cursor = engine.cursor()
    try:
        cursor.executemany(q, var)
        engine.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        raise 


# Running the script
folder = '/mnt/data/projects/aclu_leg_tracker/session_dates/'


cred_file = '../../conf/local/credentials.yaml'
engine = get_db_conn(cred_file)

## The set of files used to populate the first set of data
csv_files = {
    2011: '2011_session_calendar.csv',
    2012: '2012_session_calendar.csv',
    2013: '2013_session_calendar.csv',
    2014: '2014_session_calendar.csv',
    2015: '2015_session_calendar.csv',
    2016: '2016_session_calendar.csv',
    2017: '2017_session_calendar.csv',
    2018: '2018_session_calendar.csv',
    2019: '2019_session_calendar.csv',
    2020: '2020_session_calendar_20201231.csv',
    2021: '2021_session_calendar_20210319.csv'
}

## Updating the 2020 file and adding the 2021 (2021.03.24)
# csv_files = {
#     2020: '2020_session_calendar_20201231.csv',
#     2021: '2021_session_calendar_20210319.csv'
# }


for year, fn in csv_files.items():
    logging.info('Parsing {} for year {}'.format(fn, year))

    fpath = os.path.join(folder, fn)
    write_to_session_dates_table(engine, fpath, year, session_type='regular')



    