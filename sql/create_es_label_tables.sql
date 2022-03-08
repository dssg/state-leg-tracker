SET ROLE rg_staff;

-- Create the schema if not exists
create schema if not exists labels_es;

CREATE TABLE IF NOT EXISTS labels_es.reproductive_rights(
	bill_id INT,
	doc_id INT,
	relevance_score NUMERIC(7, 4),
	search_phrase TEXT,
	reproductive_rights INT
);

CREATE TABLE IF NOT EXISTS labels_es.criminal_law_reform(
	bill_id INT,
	doc_id INT,
	relevance_score NUMERIC(7, 4),
	search_phrase TEXT,
	criminal_law_reform INT
);

CREATE TABLE IF NOT EXISTS labels_es.immigrant_rights(
	bill_id INT,
	doc_id INT,
	relevance_score NUMERIC(7, 4),
	search_phrase TEXT,
	immigrant_rights INT
);

CREATE TABLE IF NOT EXISTS labels_es.lgbt_rights(
	bill_id INT,
	doc_id INT,
	relevance_score NUMERIC(7, 4),
	search_phrase TEXT,
	lgbt_rights INT
);


CREATE TABLE IF NOT EXISTS labels_es.racial_justice(
	bill_id INT,
	doc_id INT,
	relevance_score NUMERIC(7, 4),
	search_phrase TEXT,
	racial_justice INT
);

CREATE TABLE IF NOT EXISTS labels_es.voting_rights(
	bill_id INT,
	doc_id INT,
	relevance_score NUMERIC(7, 4),
	search_phrase TEXT,
	voting_rights INT
);