import logging
import os
import sys
import pandas as pd
import psycopg2

import yaml

from datetime import datetime
from src.utils.general import get_db_conn
from src.bill_passage.predict_forward.predict_forward import run_pipeline, _get_experiment_config

logging.basicConfig(level=logging.DEBUG, filename=f"../../logs/predict_forward_trial.log", filemode='w')
logger = logging.getLogger()


# setup
# start_date='2020-06-22'
start_date='2021-01-11'
end_date='2021-03-08' 

long_term_config_file = '../triage_config/national_long_term_predict_forward.yaml'
project_path = 's3://aclu-leg-tracker/experiment_data/bill_passage/predict_forward_trial2'
n_jobs=2

logger.info('Trial setup...')
logger.info('Running the trial from {} to {}'.format(start_date, end_date))

logger.info('Saving project details to {}'.format(project_path))
logger.info('number of processes {}'.format(n_jobs))

logger.info('Creating the date series')
# db_conn = get_db_conn('../../../conf/local/credentials.yaml')

# q = """
#     select 
#         to_char(dd, 'YYYY-MM-DD') as prediction_dates
#     from generate_series 
#         ( '{start_date}'::DATE
#         , '{end_date}'::DATE
#         , '1week'::interval) dd
# """.format(start_date=start_date, end_date=end_date)

# df = pd.read_sql(q, db_conn)

# prediction_dates = df['prediction_dates'].tolist()
# num_simulations = len(prediction_dates)

# logger.info('simulating the trial for the dates ({} modeling runs) : {}'.format(num_simulations, prediction_dates))


run_pipeline(
    credentials_file='../../../conf/local/credentials.yaml',
    config_file=long_term_config_file, 
    prediction_date='2021-10-08', 
    project_path=project_path,
    n_jobs=n_jobs,
    # replace=False
)


# Long term 
# logger.info('Long-term')
# for i, pred_date in enumerate(prediction_dates):
#     logger.info('Predicting forward for {} ({} out of {})'.format(pred_date, i+1, num_simulations))
    
#     run_pipeline(
#         config_file=long_term_config_file, 
#         prediction_date=pred_date, 
#         project_path=project_path,
#         n_jobs=n_jobs,
#         # replace=False
#     )

# short term
# logging.info('Short-term')
# for pred_date in prediction_dates:
#     logging.info('Predicting forward for {}'.format(pred_date))
#     run_pipeline(
#         config_file=short_term_config_file, 
#         prediction_date=pred_date, 
#         project_path=project_path,
#         n_jobs=n_jobs,
#         replace=False
#     )


