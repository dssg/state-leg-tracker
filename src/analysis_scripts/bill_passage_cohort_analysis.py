import os
import pandas as pd
import itertools as it
import logging

from datetime import datetime

from src.utils.general import get_db_conn

creds_folder = '../../conf/local/'
fpath = os.path.join(creds_folder, 'credentials.yaml')
db_con = get_db_conn(fpath)

cohort_q = """
with all_active_bills as (
    select 
        bill_id
    from clean.bills a
        join clean.sessions b using (session_id)
            join clean.ncsl_legiscan_linked c using (session_id)
                left join clean.bill_progress d using (bill_id)
    where 
        extract(year from date '{as_of_date}')::int in (b.year_start, b.year_end) 
        and 
        extract (year from introduced_date)::int in (b.year_start, b.year_end)
        and
        introduced_date < '{as_of_date}'
        and
        not b.special	 
        and 
        (adjourn_date > '{as_of_date}' or adjourn_date is null)
        and
        convene_date < '{as_of_date}'
        and 
        progress_date < '{as_of_date}'
    group by bill_id
    having
        max(case when event in (4, 5, 6) then progress_date end) is null
)
select
    bill_id as entity_id
from all_active_bills left join clean.bill_events using (bill_id)
where event_date < '{as_of_date}'
group by bill_id
having 
    min(('{as_of_date}'::DATE - event_date::DATE)::int) < {days_since_event_threshold}
"""



label_q = """
    with cohort as (
        {cohort_q}
    )
    select 
        entity_id,
        (CASE WHEN passed_date  < '{as_of_date}'::timestamp + interval '{label_timespan}' THEN TRUE ELSE FALSE END)::integer AS outcome
    from cohort left join (
        select 
            bill_id as entity_id, 
            min(CASE WHEN event = 4 THEN progress_date ELSE null END) AS passed_date
        from clean.bill_progress
        group by 1	
    ) as t using(entity_id)
    
"""

# as of dates
START_DATE = '2015-01-01'
END_DATE = '2021-01-01'
as_of_dates = pd.date_range(
    start=START_DATE, 
    end=END_DATE, 
    freq='7D'
).to_pydatetime().tolist()

test_grid = {
    'as_of_date': [x.strftime('%Y-%m-%d') for x in as_of_dates],    
    'label_timespan': ['3month', '6month', '1year'],
    'days_since_event_threshold': [14, 30, 60, 90],
}


def inspect_label_distribution_dismissing_inactive_bills(db_con, cohort_q, label_q, parameter_grid, save_target):
    """
        Inspect how the label distribution changes over time 
        when bills that have been inactive for a certain peiod of time is discarded from the cohort 

        args:
            cohort_q: 
            label_q:
            parameter_grid: 
            save_target: 
        return: 
            dataframe
    """

    # Converting the parameter grid into a list of parameter combinations
    all_params = sorted(parameter_grid)
    combs = it.product(*(test_grid[name] for name in all_params))
    param_combs = [dict(zip(all_params, x)) for x in combs]

    print(len(param_combs))
    # results dataframe
    results = pd.DataFrame()

    for comb in param_combs:
        comb['cohort_q'] = cohort_q.format(**comb)
        logging.info('running {}'.format(comb))
        lq = label_q.format(**comb)
        labels = pd.read_sql(lq, db_con)

        # results
        msk = labels['outcome']==1

        cohort_size = labels.shape[0]
        ones = labels[msk].shape[0]
        zeros = labels[~msk].shape[0]
        ones_prevalance = 0
        if cohort_size > 0: 
            ones_prevalance = float(ones/cohort_size)

        temp_d = {**comb}
        temp_d.pop('cohort_q', None)
        temp_d['cohort_size'] = cohort_size
        temp_d['ones'] = ones
        temp_d['zeros'] = zeros
        temp_d['ones_prevalance'] = ones_prevalance

        results = results.append(temp_d, ignore_index=True)

        print(temp_d)
        
    results.to_csv('../../data/discarding_dormant_bills_label_distribution.csv', index=False)
    
    print(results)

if __name__ == '__main__':
    inspect_label_distribution_dismissing_inactive_bills(
        db_con=db_con,
        cohort_q=cohort_q,
        label_q=label_q,
        parameter_grid=test_grid,
        save_target=None
    )




