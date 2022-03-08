# cloned_bill_text_20211119

import psycopg2

def get_states(conn):
    with conn.cursor() as cur:
        cur.execute(f'''
            SELECT initcap(state) as state, state_abbreviation as state_code from catalogs.states order by state; 
        ''')

        res = cur.fetchall()

        return {x[0]: x[1] for x in res}


def fetch_last_prediction_date(conn):
    with conn.cursor() as cur:
        q = f'''
            select
                max(as_of_date)::date as last_date
            from deploy.passage_predictions
        '''

        cur.execute(q)

        res = cur.fetchall()

        return res[0][0]