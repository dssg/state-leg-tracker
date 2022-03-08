--change role so that others can see the schema and tables
SET ROLE rg_staff;

DROP SCHEMA IF EXISTS raw CASCADE;

CREATE SCHEMA raw;

--tables

--datasets table
DROP TABLE IF EXISTS raw.datasets;

CREATE TABLE raw.datasets(
    dataset_hash varchar,
    dataset_date date,
    dataset_size integer,
    session_id varchar,
    s3_path varchar,
    uuid_value uuid
);


--sessions table
DROP TABLE IF EXISTS raw.sessions;

CREATE TABLE raw.sessions(
  session_id varchar,
  session_title varchar,
  year_start varchar,
  year_end varchar,
  state_id varchar,
  --session_hash varchar,
  special varchar
);

--bills table
DROP TABLE IF EXISTS raw.bills;

CREATE TABLE raw.bills(
  bill_id varchar,
  session_id varchar,
  bill_type varchar,
  bill_number varchar,
  state_id varchar,
  state varchar,
  subjects text,
  introduced_date date,
  introduced_body varchar,
  url varchar
);

--bill_docs table
DROP TABLE IF EXISTS raw.bill_docs;

CREATE TABLE raw.bill_docs(
  doc_id varchar,
  bill_id varchar,
  doc_type varchar,
  doc_date date,
  text_size varchar
);

--bill_events table
DROP TABLE IF EXISTS raw.bill_events;

CREATE TABLE raw.bill_events(
  bill_id varchar,
  event_date date,
  action varchar,
  chamber varchar,
  important varchar
);

--sponsors table
DROP TABLE IF EXISTS raw.bill_sponsors;

CREATE TABLE raw.bill_sponsors(
  sponsor_id varchar,
  bill_id varchar,
  party_id varchar,
  role varchar,
  sponsor_type varchar
);

--votes table
DROP TABLE IF EXISTS raw.bill_votes;

CREATE TABLE raw.bill_votes(
  vote_id varchar,
  bill_id varchar,
  vote_date date,
  description varchar,
  yea varchar,
  nay varchar,
  nv varchar,
  absent varchar,
  total varchar,
  passed varchar,
  chamber varchar,
  url text,
  state_link text
);

--if we require to know who voted what, then we will require to have
--a raw.roll_call table

--amendments table
DROP TABLE IF EXISTS raw.bill_amendments;

CREATE TABLE raw.bill_amendments(
    bill_id varchar,
    amendment_id varchar,
    amendment_date date,
    chamber varchar,
    adopted varchar,
    amendment_title text,
    amendment_description text,
    url text,
    state_link text
);

--session people table
DROP TABLE IF EXISTS raw.session_people;

CREATE TABLE raw.session_people(
    session_id varchar,
    people_id varchar,
    person_hash varchar,
    state_id smallint,
    party_id varchar,
    party varchar,
    role_id varchar,
    role varchar,
    name varchar,
    first_name varchar,
    last_name varchar,
    middle_name varchar,
    suffix varchar,
    nickname varchar,
    district varchar,
    ftm_eid varchar,
    votesmart_id varchar,
    opensecrets_id varchar,
    ballotpedia varchar,
    committee_sponsor varchar,
    committee_id varchar
);


--committee table
DROP TABLE IF EXISTS raw.bill_committees;

CREATE TABLE raw.bill_committees(
    bill_id varchar,
    committee_id varchar,
    chamber varchar,
    name varchar
);

--progress table
DROP TABLE IF EXISTS raw.bill_progress;

CREATE TABLE raw.bill_progress(
    bill_id varchar,
    progess_date date,
    event varchar
);

