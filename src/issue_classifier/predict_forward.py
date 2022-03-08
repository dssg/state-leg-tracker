import os
import sys
import logging
import psycopg2
import yaml
import pandas as pd

from datetime import datetime

from src.utils.general import (
    get_db_conn, 
    get_boto3_session, 
    get_elasticsearch_conn, 
    load_matrix_s3, load_model_s3
)
from src.issue_classifier.issue_classifier import IssueClassifier

credentials_file = '../../conf/local/credentials.yaml'

# Logger
timestr = datetime.now().strftime("%y%m%d%H%M%S")
logging.basicConfig(level=logging.DEBUG, filename=f"../../logs/issue_classifier_predict_forward_{timestr}.DEBUG", filemode='w')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# config file 
experiment_config_file = 'experiment_config/predict_forward_test.yaml'

# Models/matrices are stored
project_path = 's3://aclu-leg-tracker/experiment_data/issue_classifier/deploy'
models_used = {'reproductive_rights': 'model_hash', 'immigrant_rights': 'model_hash2'}
feature_creator_used = 'tfidf model hash'

## Steps
# 1. create time split
# 2. fetch cohort
# 3. fetch feature creator
# 4. Create the matrix
# 5. Generate preditions
# 6. Write to deploy table

# TODO: move this to a util file
def _get_experiment_config(experiment_config_file):
    """Load the configuration for the inference run"""

    # Load the config
    with open(experiment_config_file) as f:
        experiment_config = yaml.load(f)

    return experiment_config

def _set_current_modeling_dates(db_conn, experiment_config, reference_date='current_date'):
    """Updating the temporal config to reflect current predict forward setting

    Args:
        db_conn: Psycopg2 connection
        experiment_config: The configuration of the experimnt
        reference_date: The date that should be used to set the as_of_date for predicting forward. 
                        Defaults to the 'current date' (the date of prediction)
    
    """

    # In this forward predition, we are not retraining the model with the latest data
    # The label_timespan is 0 as this is a classification
    q = """
        select 
            to_char('{reference_date}'::DATE, 'YYYY-MM-DD') as feature_end_time,
            to_char('{reference_date}'::DATE, 'YYYY-MM-DD') as label_start_time,
            to_char('{reference_date}'::DATE, 'YYYY-MM-DD') as label_end_time 
    """.format( 
        reference_date=reference_date
    )

    df = pd.read_sql(q, db_conn)

    experiment_config['temporal_config']['feature_end_time'] = df.at[0, 'feature_end_time']
    experiment_config['temporal_config']['label_start_time'] = df.at[0, 'label_start_time']
    experiment_config['temporal_config']['label_end_time'] = df.at[0, 'label_end_time']

    return experiment_config


def predict_issue_area(issue_area):
    """Predict forward pipeline for a single issue area"""

    logging.info('Setting up connections')
    db_conn = get_db_conn(credentials_file)
    es_connection = get_elasticsearch_conn(credentials_file)
    s3_session = get_boto3_session(credentials_file)

    # Experiment config
    config =  _get_experiment_config(experiment_config_file)

    logging.info('Updating the modeling date')
    config = _set_current_modeling_dates(
        db_conn, 
        config, 
        reference_date='2020-06-01'
    )


    # Setup experiment
    exp = IssueClassifier(
        engine=db_conn,
        es_connection=es_connection,
        metadata_schema='issue_classifier_metadata',
        results_schema='issue_classifier_results',
        features_schema='issue_classifier_features',
        experiment_config=config,
        project_folder=project_path,
        log_file=None,
        create_matrices=True,
        matrix_exp_hash=None,
        s3_session=s3_session
    )

    exp._run_matrix_creation()

    test_matrix_uuid = _fetch_test_matrix_uuid(
        db_conn=db_conn,
        experiment_hash=exp.experiment_hash
    )   

    # load the matrix
    mat_path = '{project_path}/matrices/{matrix_uuid}.csv'.format(
        project_path=project_path,
        matrix_uuid=test_matrix_uuid
    )

    matrix = load_matrix_s3(
        s3_session, 
        matrix_path=mat_path, 
        compression=None
    )
 
    logging.info('Loading the model for prediction')
    model_path = '{project_path}/models/{model_hash}.csv'.format(
        project_path=project_path,
        model_hash=models_used[issue_area]
    )

    model = load_model_s3(s3_session, model_path)    
    model_id = _fetch_model_id(
        db_conn,
        models_used[issue_area],
        issue_area
    )


    logging.info('Scoring the bills for issue area {}'.format(issue_area))
    test_mat = _prepare_matrix(
        data_df=matrix,
        id_columns=['entity_id', 'as_of_date'],
        label_column='outcome'
    )

    predictions = model.predict_proba(test_mat.values)
    predictions = pd.DataFrame(
        predictions,
        columns=['score'],
        index=test_mat.index
    )  

    # write the predictions to the deploy schema tables
    logging.info('Writing the scores to the DB')
    _write_predictions_to_db(
        db_conn=db_conn,
        predictions=predictions,
        model_id=model_id,
        matrix_uuid=test_matrix_uuid,
        issue_area=issue_area
    )

def _prepare_matrix(data_df, id_columns=['entity_id', 'as_of_date'], label_column='outcome'):
    """Preparing the saved matrix for prediction"""

    data_df.set_index(id_columns, inplace=True)

    X = data_df.drop(
        label_column, 
        inplace=False, 
        axis=1
    ) 

    return X


def _fetch_model_id(db_conn, model_hash, issue_area):
    """Fetch the id of the used model"""

    q = """
        select 
            model_id
        from issue_classifier_metadata.models
        where model_hash='{model_hash}'
        and issue_area='{issue_area}'
    """.format(
        model_hash=model_hash,
        issue_area=issue_area
    )

    df = pd.read_sql(q, db_conn)

    return df.at[0, 'model_id']

def _write_predictions_to_db(db_conn, predictions, model_id, matrix_uuid, issue_area):
    """Write the predictions of one issue_area to the DB"""

    predictions['model_id'] = model_id
    predictions['matrix_uuid'] = matrix_uuid
    predictions['issue_area'] = issue_area

    preds_tuple = [tuple(x) for x in predictions.to_numpy()]
    cols = ', '.join(list(predictions.columns))

    cursor = db_conn.cursor()

    q = """
        insert into deploy.temp_issue_scores 
        ({}) VALUES (%s, %s, %s, %s, %s, %s)
    """.format(
        cols
    )

    try:
        cursor.executemany(q, preds_tuple)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        raise psycopg2.DatabaseError(error)


def _fetch_test_matrix_uuid(db_conn, experiment_hash):
    """Retrieveing the test matrix uuid created"""

    q = """
        select
            matrix_uuid
        from issue_classifier_metadata.matrices
        where 
        built_by_experiment='{experiment_hash}'
        and
        matrix_type='test'
    """.format(experiment_hash=experiment_hash)

    df = pd.read_sql(q, db_conn)

    return df.at[0, 'matrix_uuid']


def run_pipeline(issue_areas, models_used):
    """Run the predict forward pipeline and write the results to DB/CSV
    
    Args:
        issue_areas (List[str]): The list of issue areas we are classfifying
        models_used (Dict): The mapping between issue area and the model pickle hash used for classification 

    """

    for issue in issue_areas:
        if issue not in models_used:
            logging.error('A model is not specified for issue area {}'.format(issue))
            continue

        predict_issue_area(issue)


if __name__=='__main__':
    pass
    # run_pipeline()
