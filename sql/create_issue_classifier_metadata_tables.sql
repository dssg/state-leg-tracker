create schema if not exists issue_classifier_metadata authorization rg_staff;

set role rg_staff;

drop table if exists issue_classifier_metadata.experiments;

create table issue_classifier_metadata.experiments (
	experiment_hash varchar,
	config jsonb,
	time_splits int,
	matrices_needed int,
	models_needed int
);

drop table if exists issue_classifier_metadata.experiment_runs;

create table issue_classifier_metadata.experiment_runs (
	id serial,	
	experiment_hash varchar,
	start_time timestamp,
	run_status varchar,
	project_folder text,
	log_location text
);

drop table if exists issue_classifier_metadata.model_groups;

create table issue_classifier_metadata.model_groups (
	model_group_id serial,
	model_type text,
	hyperparameters jsonb,
	experiment_hash varchar	
);

drop table if exists issue_classifier_metadata.models;

create table issue_classifier_metadata.models (
	model_id serial,
	model_hash varchar,
	model_group_id int,
	built_by_experiment varchar,
	train_matrix_uuid varchar
); 

drop table if exists issue_classifier_metadata.matrices;

create table issue_classifier_metadata.matrices (
	matrix_id varchar,
	matrix_uuid varchar,
	matrix_type varchar,
	feature_start_time timestamp,
	lookback_duration interval,
	matrix_metadata jsonb,
	built_by_experiment varchar,
	stored_file_format varchar
)

drop table if exists issue_classifier_metadata.text_feature_groups;

create table issue_classifier_metadata.text_feature_groups (
	text_feature_group_id serial,
	feature_type text,
	hyperparameters jsonb,
	experiment_hash varchar	
);


drop table if exists issue_classifier_metadata.text_feature_creators;

create table issue_classifier_metadata.text_feature_creators (
	feature_creator_id serial,
	text_feature_group_id int,
	feature_model_hash varchar,
	built_by_experiment varchar,
	train_matrix_uuid varchar
); 

