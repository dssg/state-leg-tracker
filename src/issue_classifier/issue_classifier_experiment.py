import os
import sys
import logging
import boto3
from datetime import datetime

from src.utils.general import read_yaml_file, get_db_conn, get_elasticsearch_conn, get_s3_credentials
from src.issue_classifier.issue_classifier import IssueClassifier

timestr = datetime.now().strftime("%y%m%d%H%M%S")

# log file
logs= "../../logs"
log_file = "issue_classifier_{}.DEBUG".format(timestr)
logging.basicConfig(level=logging.DEBUG, filename=os.path.join(logs, log_file), filemode='w')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# Connections
cred_file = '../../conf/local/credentials.yaml'
engine = get_db_conn(cred_file)
es = get_elasticsearch_conn(cred_file)

s3_creds = get_s3_credentials(cred_file)
s3_session = boto3.Session(
    aws_access_key_id=s3_creds['aws_access_key_id'],
    aws_secret_access_key=s3_creds['aws_secret_access_key']
)

# experiment configuration
configs = os.path.join('experiment_config')

# reading the config file from user input
config_file = sys.argv[1]

# Reading the folder path to save the experiment components
project_folder = sys.argv[2]

# Reading whether to create matrices or use existing ones from user input
# If using existing ones, the experiment hash where the matrices were created should be given
try:
    replace = sys.argv[3]
    if replace == 't':
        rep = True
        mat_exp_hash = None
    else:
        rep = False
        mat_exp_hash = sys.argv[4]
except:
    rep = True
    mat_exp_hash = None

config = read_yaml_file(config_file)


exp = IssueClassifier(
        engine=engine,
        es_connection=es,
        metadata_schema='issue_classifier_metadata',
        results_schema='issue_classifier_results',
        features_schema='issue_classifier_features',
        experiment_config=config,
        project_folder=project_folder,
        log_file=log_file,
        create_matrices=rep,
        matrix_exp_hash=mat_exp_hash,
        s3_session=s3_session
)

exp.run()
