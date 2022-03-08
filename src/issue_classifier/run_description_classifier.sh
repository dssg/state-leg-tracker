config_folder=experiment_config/
config_file1=first_test_lgbt.yaml
config_file2=first_test_racial.yaml
config_file3=first_test_reproductive.yaml

project_folder=/mnt/data/experiment_data/aclu/issue_classify_tests/first_complete_test/

replace=f

exp_hash=48578ac6c92cc2fb0ca5ae68d12241ed

python issue_classifier_experiment.py $config_folder$config_file1 $project_folder $replace $exp_hash
python issue_classifier_experiment.py $config_folder$config_file2 $project_folder $replace $exp_hash
python issue_classifier_experiment.py $config_folder$config_file3 $project_folder $replace $exp_hash

