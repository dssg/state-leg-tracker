import pandas as pd
from datetime import date 

from src.utils.general import get_db_conn, copy_df_to_pg
from src.utils.project_constants import ISSUE_AREAS


"""This script generates a CSV that is sent to ACLU for labeling"""

db_conn = get_db_conn('../../../conf/local/credentials.yaml')

num_of_bills = 100
# issue_areas = ['criminal_justice', 'voting_rights', 'racial_justice', 'immigrants_rights', 'tech_privacy_civil_liberties', 'lgbtq_rights', 'other']

q = """
    with unlabeled_docs as (
        select 
            doc_id,
            bill_id
        from clean.bill_docs a 
        WHERE NOT EXISTS (
            SELECT 
                distinct doc_id
            FROM aclu_labels.issue_areas b
            WHERE a.doc_id = b.doc_id
        )
    )
    select 
        doc_id,
        bill_id,
        bill_number,
        state,
        extract(year from introduced_date)::int as "year"
    from unlabeled_docs join clean.bills b using(bill_id)
    where state !='US'
    order by random() limit {}
""".format(num_of_bills)

bills_to_label = pd.read_sql(q, db_conn)


# We have to format the link into a specific format to point the labeler to the text
# e.g. -- https://legiscan.com/CA/text/AB1108/id/10s0


def format_url(row):
    s = 'https://legiscan.com/{state}/text/{bill_number}/id/{doc_id}'
    
    return s.format(
        state=row['state'],
        bill_number=row['bill_number'],
        doc_id=row['doc_id']
    )

# bills_to_label.apply(format_url, axis=1)
bills_to_label['url'] = bills_to_label.apply(format_url, axis=1)

labels_df = pd.DataFrame(columns=ISSUE_AREAS)

# appending the issue area columns
bills_to_label = bills_to_label.join(labels_df)

# adding the Notes column
bills_to_label['notes'] = None

# file name
fdate = date.today().strftime("%Y%m%d")
fname = '/mnt/data/projects/aclu_leg_tracker/data_from_aclu/issue_area_labels/sent_for_labeling/bills_to_label_{}.csv'.format(fdate)

# TODO
# write the dataframe to a google sheet
# saving the CSV
bills_to_label.to_csv(fname, index=False)

# Writing the files created to the labels table to ommit repeating
columns_to_copy = ['doc_id']
copy_df_to_pg(
    engine=db_conn,
    table_name='aclu_labels.issue_areas',
    df=bills_to_label,
    columns_to_write=columns_to_copy
)

