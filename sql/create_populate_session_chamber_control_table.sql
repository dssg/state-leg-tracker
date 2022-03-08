-- This script creates a table called a session_chamber_control in the pre_triage_features schema
-- This table is used for bill passage models in triage
-- While it's possible to create it on the fly through triage 'from_obj, creating it a-priori for efficiency purposes
SET ROLE rg_staff;

CREATE SCHEMA IF NOT EXISTS pre_triage_features;

DROP TABLE IF EXISTS pre_triage_features.session_chamber_control_party;

CREATE TABLE pre_triage_features.session_chamber_control_party (
	session_id int,
	chamber_id int,
	controlling_party_id int,
	chamber_size int,
	primary key (session_id, chamber_id)
);

INSERT INTO pre_triage_features.session_chamber_control_party (
    SELECT 
		DISTINCT ON(session_id, role_id) session_id, role_id AS chamber_id, party_id AS controlling_party_id, num_members AS chamber_size
	FROM (
		SELECT 
			session_id, role_id, party_id, count(DISTINCT people_id) AS num_members
		FROM clean.session_people
		GROUP BY 1, 2, 3
	) AS t 
	ORDER BY session_id, role_id, num_members DESC
);