-- This script creates the tables in the deploy schema
-- The deploy schema contains the predict forward scores for all the models
CREATE SCHEMA IF NOT EXISTS deploy;

-- Truncating and dropping the table
TRUNCATE TABLE IF EXISTS deploy.passage_predictions
DROP TABLE IF EXISTS deploy.passage_predictions;

create table deploy.passage_predictions (
	model_id integer,
	matrix_uuid varchar,
	bill_id integer,
	as_of_date timestamp,
	label_timespan interval,
	score numeric(6, 5),
	rank_pct numeric(6, 5),
    PRIMARY KEY (model_id, bill_id, as_of_date)
)
