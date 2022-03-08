import pandas as pd

def retrieve_states_catalogue(db_conn):
    """
    Retrieves the states catalogue
    :param db_conn:
    :return:
    """

    q = """ 
    select state_id, state_abbreviation, state 
    from clean.states
    """

    states_catalogue = pd.read_sql(q, db_conn)

    return states_catalogue


def retrieve_reproductive_rights_bills(db_conn):
    """
    Retrieves the main metadata on bills from reproductive rights
    :param db_conn: Connection to DB
    :return:
    """

    q = """
    with step_1 as(
      select bill_id, max(doc_date) as doc_date, max(doc_id) as doc_id
      from temp_eda.repro_labels_all
      group by bill_id 
    )
      select a.bill_id, a.doc_id, a.doc_date, text_size
      from temp_eda.repro_labels_all a
      inner join step_1 b
      on a.bill_id = b.bill_id 
      and a.doc_date = b.doc_date
      and a.doc_id = b.doc_id
    """

    reproductive_rights_bills = pd.read_sql(q, db_conn)
    reproductive_rights_bills.drop_duplicates(inplace=True)

    return reproductive_rights_bills


def retrieve_bill_metadata(db_conn, df):
    """
    Retrieves all metadata from bills
    :param db_conn: Connection to database
    :param df: Dataframe with bills from reproductive rights
    :return:
    """

    q = """
    select bill_id, state, session_id, session_name, session_special, session_title, session_year_start,
    session_year_end, status
    from temp_eda.bills_metadata;
    """

    all_data = pd.read_sql(q, db_conn)
    all_data['bill_id'] = all_data.bill_id.astype(int)
    all = all_data.merge(df, how="inner", on="bill_id")

    return all