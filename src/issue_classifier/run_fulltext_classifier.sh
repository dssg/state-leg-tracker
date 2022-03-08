config_folder=experiment_config/
config_file=first_full_text_remaining.yaml

project_folder=/mnt/data/experiment_data/aclu/issue_classify_tests/first_full_text_test/

replace=f

exp_hash=1736a4534070b0a6550e30845130d39c

python issue_classifier_experiment.py $config_folder$config_file $project_folder $replace $exp_hash
