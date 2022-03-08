import os
import sys
import pandas as pd
import psycopg2
import logging
import yaml

from datetime import datetime, timedelta
from sqlalchemy.engine.url import URL
from triage import create_engine
from triage.experiments import SingleThreadedExperiment
from triage.experiments import MultiCoreExperiment

from src.utils.general import get_db_conn, copy_df_to_pg

creds_folder = '../../conf/local/'
credentials_file = os.path.join(creds_folder, 'credentials.yaml')


def _get_serializable_db_conn(credentials_file):
    """Setup the serializable sql engine to be used with multicore triage"""
    # db connection
    with open(credentials_file) as f:
        db_config = yaml.load(f)['db']

    db_url = URL(
        'postgres',
        host=db_config['host'],
        username=db_config['user'],
        database=db_config['db'],
        password=db_config['pass'],
        port=db_config['port'],
    )

    engine = create_engine(db_url)

    return engine


def _get_experiment_config(experiment_config_file):
    """Load the configuration for the inference run"""

    # Load the config
    with open(experiment_config_file) as f:
        experiment_config = yaml.load(f)

    return experiment_config


# TODO: Set the reference date as a parameter
def _set_current_modeling_dates(db_conn, config, prediction_date='current_date'):
    """ Setting the modeling dates to set the correct timechop for predicting forward.

    Args:
        db_conn : SQL connection
        config: The triage experiment config used for predicting forward
        prediction_date (str): The 'as_of_date' that should be used for prediction. This defaults to the 'current_date', which is the date the pipeline is run. 
                        In case a different date is used, it should be given in the format 'YYYY-MM-DD'

    return:
        modified config
    """
    
    # Assuming that there's only one label_timespan
    label_timespan = config['temporal_config']['label_timespans'][0]

    # Assuming only one max training history range
    max_training_history = config['temporal_config']['max_training_histories'][0] 
    
    # The label start and end times dictate the number of time splits generated
    # In this setup, we are creating one train set, and one testset. The test set contains one as_of_date and 
    # the train set spans some time in the history (2 years).
    # The selected model group is trained on the test set and the test set is used to predict forward
    # The label start time is set to <current_date> - (<label timespan> + <training history>) to reserve the space for training labels
    # The label end time is set to the future as we are predicting forward.
    # e.g. if the modeling date is 2020-11-01, max training history is 2 years, and label timespan is 1 month, 
    # label start time would be 2018-10-01, label end time would be 2020-12-01
    
    # TODO: We are not doing a validation setup here. Maybe should manipulate the model_update_frequency to 
    #       give us another model that enables us to run a validation check before we publish results

    # TODO: We are currently using the current modeling date as the reference point. 
    # Alternatively, we could use a reference point from the data. The query could take the following form
    # sql = 
    # with most_recent as (
    #     select 
    #         max(event_date) as last_event_date
    #         from clean.bill_events 
    #         where event_date < current_date
    #     )
    #     select 
    #         to_char(last_event_date, 'YYYY-MM-DD') as feature_end_time,
    #         to_char(last_event_date - interval '{label_timespan}'- interval '{max_training_history}', 'YYYY-MM-DD') as label_start_time,
    #         to_char(last_event_date + interval '{label_timespan}', 'YYYY-MM-DD') as label_end_time
    #     from most_recent
    
  
    q = """
        select 
        to_char('{prediction_date}'::DATE, 'YYYY-MM-DD') as feature_end_time,
        to_char('{prediction_date}'::DATE - interval '{label_timespan}' - interval '{max_training_history}' - interval '{label_timespan}', 'YYYY-MM-DD') as label_start_time,
        to_char('{prediction_date}'::DATE + interval '{label_timespan}', 'YYYY-MM-DD') as label_end_time
    """.format(prediction_date=prediction_date, label_timespan=label_timespan, max_training_history=max_training_history)

    df = pd.read_sql(q, db_conn)
    config['temporal_config']['feature_end_time'] = df.at[0, 'feature_end_time']
    config['temporal_config']['label_start_time'] = df.at[0, 'label_start_time']
    config['temporal_config']['label_end_time'] = df.at[0, 'label_end_time']
    config['temporal_config']['model_update_frequency'] = label_timespan

    return config


def _fetch_test_matrix_uuid(db_conn, experiment_hash):
    """ fetch the uuid of the generated test marix. The temporal config is setup so that only one test matrix"""
    q = """
        select 
            matrix_uuid 
        from 
            triage_metadata.matrices 
        where 
            built_by_experiment='{}' 
            and 
            matrix_type='test'
    """.format(experiment_hash)

    df = pd.read_sql(q, db_conn)

    mat_uuid = df['matrix_uuid'].iloc[0]

    return mat_uuid


def run_pipeline(credentials_file, config_file, project_path, prediction_date='current_date', n_jobs=1):
    """ Run the inference pipeline for bill passage
    
    Args:
        credentials_file: The YAML file where the DB credentials are given. Should contain a section called 'DB'
        config_file: The triage experiment config file used for generating predictions
        project_path: The disk location or S3Bucket where the models and matrices would be saved by triage
        prediction_date: The modeling date we use to set up the temporal config. 
            defaults to the the date where the pipeline is run
        n_jobs: Number of processes to be used by Triage
    """

    logging.info('Setting up the connections')
    db_conn_triage = _get_serializable_db_conn(credentials_file)
    db_conn = get_db_conn(credentials_file)
    config = _get_experiment_config(config_file)
    
    logging.info('Updating the modeling dates to today')
    config = _set_current_modeling_dates(db_conn_triage, config, prediction_date=prediction_date)

    # TODO: Updating the config file on disk with new dates
    with open('last_used_config.yaml', 'w') as f:
        yaml.dump(config, f)
    
    logging.info('The new temporal config {}'.format(config['temporal_config']))

    logging.info('Setting up the experiment')
    if n_jobs > 1:
        experiment = MultiCoreExperiment(
            config=config,
            db_engine=db_conn_triage,
            n_processes=n_jobs,
            n_db_processes=n_jobs,
            project_path=project_path,
            replace=True,
            save_predictions=True
        )
    else:
        experiment = SingleThreadedExperiment(
            config=config,
            db_engine=db_conn_triage,
            project_path=project_path,
            replace=True,
            save_predictions=True
        )

    # Only using the last two time splits given by timechop as we need 1 predict forward split, and one validation split
    if len(experiment.split_definitions) > 2:
        experiment.split_definitions = experiment.split_definitions[-2:]
    logging.info(
        '''Timechop creates more than two time splits when we actually need two.
        So we are hacking into the split definitions and retaining only the last two splits.
        Kinda gross, but easiest thing to do!'''
    )
    logging.info('Test as_of_dates for the retained splits {}'.format(
        [x['test_matrices'][0]['as_of_times'] for x in experiment.split_definitions]
    ))
    
    logging.info('Creating the cohort and the labels')
    experiment.generate_cohort()
    experiment.generate_labels()
    
    logging.info('Feature generation and imputation')
    experiment.generate_preimputation_features()
    experiment.impute_missing_features()

    logging.info('Generating the matrices')
    experiment.build_matrices()

    logging.info('Training and predict forward')
    experiment.train_and_test_models()

    predictions = _write_to_deploy_db(db_conn, experiment.experiment_hash)

    logging.info('Writing the predictions to a CSV')
    write_results_to_csv(db_conn, prediction_date)


def _write_to_deploy_db(engine, experiment_hash):
    """ Writing the scores the deploy db """

    # Fetch predictions
    # We are copying only the predictions from the predict forward model to the deploy schema
    # Not the validation split
    q = """
        with trained_models as (
            select 
                model_id 
            from triage_metadata.experiment_models join triage_metadata.models using(model_hash) 
            where experiment_hash='{}'
            order by train_end_time desc limit 1 -- Last train_end_time for the 
        )
        select 
            model_id, 
            matrix_uuid, 
            entity_id as bill_id, 
            as_of_date, 
            score, 
            test_label_timespan as label_timespan
        from test_results.predictions join trained_models using(model_id)
        order by score desc
    """.format(experiment_hash)

    predictions = pd.read_sql(q, engine)
    predictions['rank_pct'] = predictions['score'].rank(
        method='min',
        pct=True
    )

    try:
        copy_df_to_pg(
            engine=engine,
            table_name='deploy.passage_predictions',
            df=predictions
        )
    except (Exception, psycopg2.DatabaseError) as error:
        logging.warning(error)

    # TODO: Add a check whether the results were already entered
    # Writing to the deploy database
    # q = """
    #     INSERT INTO deploy.passage_predictions
    #     (model_id, matrix_uuid, bill_id, as_of_date, label_timespan, score, rank_pct)
    #     VALUES ({});
    # """.format(
    #     ', '.join(['%s'] * 7)
    # )

    # preds_tuple = [tuple(x) for x in predictions.to_numpy()]

    # cursor = engine.cursor() 

    # try:
    #     cursor.executemany(q, preds_tuple)
    #     engine.commit()
    # except (Exception, psycopg2.DatabaseError) as error:
    #     logging.error(error)
    #     raise psycopg2.DatabaseError(error)

    return predictions


# TODO: This function will move to the script that compiles and creates the final CSV version
def write_results_to_csv(engine, prediction_date='currrent_date'):
    """ Write the results to a csv file for easy download should be called after writing results to the DB"""

    # TODO: This query should be updated with the current date and to in
    q = """
        with model_predictions as (
            select * from deploy.passage_predictions where as_of_date='{prediction_date}'
        )
        select
            bill_id, 
            as_of_date as prediction_date, 
            bill_type, 
            state, 
            session_title, 
            score,
            year_start as session_start_year, 
            year_end as session_end_year, 
            introduced_date, 
            introduced_body,
            url as legiscan_url
        from model_predictions join clean.bills using (bill_id)
            join clean.sessions using(session_id)
    """.format(
        prediction_date=prediction_date
    )

    results_df = pd.read_sql(q, engine)

    if prediction_date=='current_date':
        datestamp = datetime.now().strftime("%Y%m%d")
    else:
        datestamp = prediction_date

    results_df.to_csv('../../results/{}_national_scores.csv'.format(datestamp), index=False)
