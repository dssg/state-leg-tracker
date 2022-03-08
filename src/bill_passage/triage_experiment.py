import os
import sys
import yaml
import logging
import ntpath
import getpass
import pandas as pd

from datetime import datetime
from triage.experiments import SingleThreadedExperiment
from triage.experiments import MultiCoreExperiment
from triage.component.timechop.timechop import Timechop
from triage.component.timechop.plotting import visualize_chops
from triage import create_engine
from sqlalchemy.engine.url import URL

from src.utils.general import get_db_conn
from src.utils.project_constants import DATA_FOLDER

logging.basicConfig(level=logging.DEBUG, filename="../../logs/triage_passage_nat_lt.DEBUG", filemode='w')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


def _setup_experiment(experiment_config_file, credentials_file):
    """ Setup the sql engine, project folder, and config for the experiment"""
    
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
    
    with open(experiment_config_file) as f:
        experiment_config = yaml.load(f)

    # cf = ntpath.basename(experiment_config_file)[0:10]
    # project_folder = os.path.join(DATA_FOLDER, 'aclu', 'triage_tests', '{}_{}/'.format(timestr, cf))

    # logging.info(project_folder)


    return experiment_config, engine


def create_subsets(engine):
    """creates the list of dictionaries that contain the evaluation subsets"""

    # list of dictionaries
    subsets_list = list()

    # States
    q = "select replace(state, ' ', '_') as state from catalogs.states"
    states = pd.read_sql(q, engine)['state'].to_list()

    state_query = "SELECT bill_id as entity_id FROM clean.bills WHERE state='{state}'"
    temp = [
        {'name': x, 'query': state_query.format(state=x) + " AND introduced_date < '{as_of_date}'"} for x in states
    ]
    subsets_list = subsets_list + temp

    # Bill type 
    q = """SELECT lower(bill_type) as bill_type from catalogs.bill_types"""
    bill_types = pd.read_sql(q, engine)['bill_type'].to_list()

    bill_type_query = "SELECT bill_id as entity_id FROM clean.bills WHERE bill_type='{bill_type}'"
    temp = [
        {'name': 'bt_' + x, 'query': bill_type_query.format(bill_type=x) + " AND introduced_date < '{as_of_date}'"} for x in bill_types
    ]
    subsets_list = subsets_list + temp

    # Bill age
    temp = [
        {
            'name': 'at_intro', 
            'query': "SELECT bill_id as entity_id FROM clean.bills WHERE introduced_date BETWEEN ('{as_of_date}'::date - '1d'::interval) and ('{as_of_date}'::date)"
        },
        {
            'name': 'in_1d', 
            'query': "SELECT bill_id as entity_id FROM clean.bills WHERE introduced_date BETWEEN ('{as_of_date}'::date - '2d'::interval) and ('{as_of_date}'::date)"
        },
        {
            'name': 'within_1w', 
            'query': "SELECT bill_id as entity_id FROM clean.bills WHERE introduced_date BETWEEN ('{as_of_date}'::date - '1week'::interval) and ('{as_of_date}'::date)"
        },
        {
            'name': 'within_2w', 
            'query': "SELECT bill_id as entity_id FROM clean.bills WHERE introduced_date BETWEEN ('{as_of_date}'::date - '2week'::interval) and ('{as_of_date}'::date)"
        },
        {
            'name': 'within_1m', 
            'query': "SELECT bill_id as entity_id FROM clean.bills WHERE introduced_date BETWEEN ('{as_of_date}'::date - '1month'::interval) and ('{as_of_date}'::date)"
        },
        {
            'name': 'older_than_1m', 
            'query': "SELECT bill_id as entity_id FROM clean.bills WHERE introduced_date < ('{as_of_date}'::date - '1m'::interval)"
        },
        {
            'name': 'older_than_3m', 
            'query': "SELECT bill_id as entity_id FROM clean.bills WHERE introduced_date < ('{as_of_date}'::date - '3m'::interval)"
        },
        {
            'name': 'older_than_6m', 
            'query': "SELECT bill_id as entity_id FROM clean.bills WHERE introduced_date < ('{as_of_date}'::date - '6m'::interval)"
        }
    ]
    subsets_list = subsets_list + temp

    return subsets_list


def run_exp(experiment_config_file, credentials_file, project_folder, plot_timechops=True, only_validate=False, n_jobs=1):
    """
        Run the triage experiment for bil passage
        args:
            experiment_config_file: Triage configuration file
            credentials_file: Credentials required for the db connection
            project_folder: The target for saving matrices and models (disk or S3)
            plot_timechops: Whether to visualize the timechops or not
            only_validate: Whether to only vaidate the experiment
            n_jobs: Number of processes used
    """
    
    experiment_config, engine = _setup_experiment(experiment_config_file, credentials_file) 
    subsets = create_subsets(engine=engine)
    experiment_config['scoring']['subsets'] = subsets
    
    # print(len(experiment_config['feature_aggregations']))
    # print(experiment_config['feature_aggregations'][0])

    # for feat in experiment_config['feature_aggregations']:
    #     print(feat['prefix'])
    #     for agg in feat['aggregates']:
    #         print(agg['quantity'])

    # return 

    if n_jobs > 1:
        experiment = MultiCoreExperiment(
            config=experiment_config,
            db_engine=engine,
            n_processes=n_jobs,
            n_db_processes=8,
            project_path=project_folder,
            replace=False,
            save_predictions=False
        )
    else:
        experiment = SingleThreadedExperiment(
            config=experiment_config,
            db_engine=engine,
            project_path=project_folder,
            replace=False,
            save_predictions=False
        )
    
    experiment.validate()

    if not only_validate:
        experiment.run()

    
    if visualize_chops:
        save_target = os.path.join(project_folder, 'timechop_most_recent_bill_passage_run.png')
        _visualize_timechop(experiment_config['temporal_config'], save_target=save_target)
        

def _visualize_timechop(temporal_config, save_target=None):
    """ Plotting the timechops """

    chopper = Timechop(**temporal_config)

    if save_target is None:
        save_target = 'timechop.png'

    visualize_chops(
        chopper=chopper,
        show_as_of_times=True,
        show_boundaries=True,
        save_target=None
    )


if __name__ == '__main__':
    config_file = sys.argv[1]
    credentials_file = sys.argv[2]
    project_folder = sys.argv[3]
    n_jobs = sys.argv[4]

    logging.info('Running the experiment from {} with {} processes'.format(
            config_file, n_jobs 
        )
    )

    run_exp(
        config_file, 
        credentials_file,
        project_folder,
        plot_timechops=False, 
        only_validate=False,
        n_jobs=int(n_jobs)
    )
