-- Session dates
-- contains dates obtained by NCSL
DROP TABLE IF EXISTS raw.session_dates;

CREATE TABLE raw.session_dates (
	session_entry_id serial, -- not using the name session_id as it is already used in legiscan data
	session_year,
    state_name varchar,
	convene_date date,
	adjourn_date date,
	special boolean,
	notes text
);