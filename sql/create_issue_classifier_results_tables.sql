set role rg_staff;

create schema if not exists issue_classifier_results authorization rg_staff;

create table issue_classifier_results.train_predictions (
	model_id int,
	matrix_uuid varchar,
	experiment_hash varchar,
	entity_id int,
	as_of_date timestamp,
	issue_area varchar,
	score numeric(6,5),
	label_value smallint,
	label_timespan interval -- In case there are multiple label timestamps in the same experiment
)

create table issue_classifier_results.train_evaluations (
	model_id int,
	evaluation_start timestamp,
	evaluation_end timestamp,
	metric varchar,
	parameter varchar,
	value numeric(6,5),
	num_labeled_examples int,
	num_labeled_above_threshold int,
	num_positive_labels int
)

create table issue_classifier_results.test_predictions (
	model_id int,
	matrix_uuid varchar,
	experiment_hash varchar,
	entity_id int8,
	as_of_date timestamp,
	issue_area varchar,
	score numeric(6,5),
	label_value smallint,
	label_timespan interval -- In case there are multiple label timestamps in the same experiment
)

create table issue_classifier_results.test_evaluations (
	model_id int,
	evaluation_start timestamp,
	evaluation_end timestamp,
	metric varchar,
	parameter varchar,
	value numeric(6,5),
	num_labeled_examples int,
	num_labeled_above_threshold int,
	num_positive_labels int
)