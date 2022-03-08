import boto3
import os
import pandas as pd
import matplotlib.pyplot as plt

from wordcloud import WordCloud

from src.utils.general import get_db_conn, get_s3_credentials
from src.utils.eda import retrieve_states_catalogue
from src.utils import project_constants as constants


def _retrieve_reproductive_bills_per_state(state, db_conn):
    """
     Retrieve the ids from reproductive bills per state
    :param state: State from which to retrieve bill of reproductive bills for
    :param db_conn: Connection to database
    :return:
    """

    q = """
    with step_1 as(
      select cast(bill_id as varchar), max(doc_date) as doc_date, max(doc_id) as doc_id
      from temp_eda.repro_labels_all
      group by bill_id 
    ),

    step_2 as(
      select cast(a.bill_id as varchar), a.doc_id, a.doc_date, text_size
      from temp_eda.repro_labels_all a
      inner join step_1 b
      on cast(a.bill_id as varchar) = b.bill_id 
      and a.doc_date = b.doc_date
      and a.doc_id = b.doc_id
    )

    select b.bill_id, doc_id, doc_date, state
    from temp_eda.bills_metadata a
    inner join step_2 b
    on a.bill_id = b.bill_id
    where state = '{}';
    """.format(state)

    reproductive_rights_bills = pd.read_sql(q, db_conn)
    reproductive_rights_bills['bill_id'] = reproductive_rights_bills['bill_id'].astype(str)
    bill_ids = tuple(reproductive_rights_bills.bill_id.values.tolist())

    return bill_ids


def _retrieve_terms_reproductive_rights_bills(bill_ids, db_conn):
    """
    Retrieve the terms of all the bills from reproductive rights
    :param bill_ids: List of bill ids to retrieve
    :param db_conn: Connection to database
    :return:
    """

    q = """
       select bill_id, doc_id, term
       from clean.bill_tokens_tf_df 
       where bill_id in {}
       """.format(bill_ids)

    tokens_reproductive_rights = pd.read_sql(q, db_conn)
    #tokens_reproductive_rights.to_pickle("tokens_repro_" + state + ".pkl")
    tokens_repro_state = tokens_reproductive_rights.term.values.tolist()
    text = " ".join(tokens_repro_state)

    return text


def _save_wordcloud_s3(word_cloud, state):
    """
    s
    :param word_cloud:
    :param state:
    :return:
    """
    s3_credentials = get_s3_credentials("../../../conf/local/credentials.yaml")

    session = boto3.Session(
        aws_access_key_id=s3_credentials['aws_access_key_id'],
        aws_secret_access_key=s3_credentials['aws_secret_access_key']
    )

    s3 = session.resource('s3')
    s3_bucket = constants.S3_BUCKET
    s3_route = '{}/{}/'.format("eda", "word_clouds")
    file_name = "wordcloud_repro_" + state + '.png'

    word_cloud.to_file("wordcloud_repro_" + state + ".png")

    # save csv on s3
    s3.meta.client.upload_file(file_name, Bucket=s3_bucket, Key=s3_route + file_name)
    os.remove(file_name)


def generate_word_cloud():
    """

    :return:
    """
    db_conn = get_db_conn("../../../conf/local/credentials.yaml")

    states = retrieve_states_catalogue(db_conn)
    for state in states.state_abbreviation.values:
        print(state)
        bill_ids = _retrieve_reproductive_bills_per_state(state, db_conn)
        text = _retrieve_terms_reproductive_rights_bills(bill_ids, db_conn)

        plt.clf()
        wordcloud_repro_state = WordCloud(background_color="white").generate(text)
        plt.figure()
        plt.imshow(wordcloud_repro_state, interpolation="bilinear")
        plt.title(state)
        plt.axis("off")

        _save_wordcloud_s3(wordcloud_repro_state, state)


generate_word_cloud()



