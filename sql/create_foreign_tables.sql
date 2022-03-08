--change role so that others can see the schema and tables
SET ROLE rg_staff;

--bill_text index table
DROP FOREIGN TABLE IF EXISTS raw.bill_text_es;

CREATE FOREIGN TABLE raw.bill_text_es(
  doc_id varchar,
  doc_date date,
  type varchar,
  type_id smallint,
  mime varchar,
  mime_id smallint,
  url text,
  state_link text,
  text_size integer,
  bill_id varchar,
  doc text,
  title text,
  description text
)
SERVER multicorn_es
OPTIONS(
  index 'bill_text'
);

--bill_meta index table
DROP FOREIGN TABLE IF EXISTS raw.bill_meta_es;

CREATE FOREIGN TABLE raw.bill_meta_es(
  bill_id varchar,
  bill_number varchar,
  bill_type varchar,
  bill_type_id smallint,
  amendments text,
  body varchar,
  body_id smallint,
  change_hash varchar,
  committee jsonb,
  current_body varchar,
  current_body_id smallint,
  description text,
  history text,
  progress text,
  session jsonb,
  sponsors text,
  state varchar,
  state_id smallint,
  state_link varchar,
  status smallint,
  status_date date,
  subjects text,
  title varchar,
  url varchar,
  votes text
)
SERVER multicorn_es
OPTIONS(
  index 'bill_meta'
);

--session_people index table
DROP FOREIGN TABLE IF EXISTS raw.session_people_es;

CREATE FOREIGN TABLE raw.session_people_es(
  session_id smallint,
  people_id varchar,
  person_hash varchar,
  state_id smallint,
  party_id smallint,
  party varchar,
  role_id smallint,
  role varchar,
  name varchar,
  first_name varchar,
  middle_name varchar,
  last_name varchar,
  suffix varchar,
  nickname varchar,
  district varchar,
  ftm_eid varchar,
  votesmart_id varchar,
  openscrets_id varchar,
  ballotpedia varchar,
  committee_sponsor varchar,
  committee_id varchar
)
SERVER multicorn_es
OPTIONS(
  index 'session_people'
);
