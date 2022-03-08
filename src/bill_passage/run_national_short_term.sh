#!/bin/bash

config_folder=triage_config/
config_file=national_short_term_master.yaml

project_folder=s3://aclu-leg-tracker/experiment_data/bill_passage/triage/national_short_term_master

# where the credentials file is stored
credentials_file=../../conf/local/credentials.yaml

n_jobs=16

python triage_experiment.py $config_folder$config_file $credentials_file $project_folder $n_jobs
