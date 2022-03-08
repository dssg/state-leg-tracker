config_folder=experiment_config/
config_file=models_for_deployment_test.yaml

project_folder=s3://aclu-leg-tracker/experiment_data/issue_classifier/models_for_predict_forward_trial

replace=t

# exp_hash=1736a4534070b0a6550e30845130d39c

python issue_classifier_experiment.py $config_folder$config_file $project_folder $replace $exp_hash
