import pandas as pd
import logging
import psycopg2

from src.utils.general import get_db_conn
from src.utils.project_constants import ISSUE_AREAS, ISSUE_AREA_LABEL_SET



db_conn = get_db_conn('../../../conf/local/credentials.yaml')

labeled_file_loc = "/mnt/data/projects/aclu_leg_tracker/data_from_aclu/issue_area_labels"
file_name = "labeled_bills_20210622.xlsx"
labeling_date = '2021-06-22'


df = pd.read_excel('{}/{}'.format(labeled_file_loc, file_name),  engine='openpyxl')


def update_labels(row):
    """Update the labels in the DB for one bill doc"""

    q = "UPDATE aclu_labels.issue_areas SET"

    for issue_area in ISSUE_AREAS:
        if row[issue_area] not in ISSUE_AREA_LABEL_SET:
            continue
        q += "\n\t{}='{}', ".format(issue_area, row[issue_area])

    # notes, labeling date, and the labeler
    # handling the single qutes in the notes
    row['notes'] = str(row['notes']).replace("'", "''")

    q += """
        notes='{}', 
        labeling_date='{}',
        labeler='{}'
    """.format(
        row['notes'],
        labeling_date,
        row['assigned to']
    )


    # where
    q += "\nWHERE doc_id={}".format(row['doc_id'])

    cursor = db_conn.cursor()

    logging.info('Updating the issue areas of bill version {}'.format(row['doc_id']))

    try:
        cursor.execute(q)
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        raise error
        # db_conn.rollback()


df.apply(update_labels, axis=1)




