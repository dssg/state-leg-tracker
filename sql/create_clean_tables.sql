--change role so that others can see the schema and tables
SET ROLE rg_staff;

--clean schema
DROP SCHEMA IF EXISTS clean CASCADE;

CREATE SCHEMA clean;

--status of bills catalogue
DROP TABLE IF EXISTS clean.bill_status;

CREATE TABLE clean.bill_status(
  status_id smallint,
  status varchar
);


--tables from raw schema
--bill_amendments table
DROP TABLE IF EXISTS clean.bill_amendments;

CREATE TABLE clean.bill_amendments(
    bill_id integer,
    amendment_id integer,
    amendment_date date,
    chamber varchar,
    adopted smallint,
    amendment_title text,
    amendment_description text,
    url text,
    state_link text,
    primary key (bill_id, amendment_id)
);

--bill_docs table
DROP TABLE IF EXISTS clean.bill_docs;

CREATE TABLE clean.bill_docs(
    doc_id integer,
    bill_id integer,
    doc_type varchar,
    doc_date date,
    text_size integer,
    mime_id integer,
    url text,
    state_link text,
    primary key (doc_id, bill_id)
);

--bill_committees table
DROP TABLE IF EXISTS clean.bill_committees;

CREATE TABLE clean.bill_committees(
    bill_id integer,
    committee_id integer,
    chamber varchar,
    name varchar,
    primary key (bill_id, committee_id)
);


--bill_events table
DROP TABLE IF EXISTS clean.bill_events;

CREATE TABLE clean.bill_events(
    bill_id integer,
    event_date date,
    action varchar,
    chamber varchar,
    important smallint,
    event_hash varchar,
    primary key (bill_id, event_hash)
);

--bill_progress table
DROP TABLE IF EXISTS clean.bill_progress;

CREATE TABLE clean.bill_progress(
    bill_id integer,
    progress_date date,
    event smallint,
    primary key (bill_id, progress_date, event)
);

--bill_sponsors table
DROP TABLE IF EXISTS clean.bill_sponsors;

CREATE TABLE clean.bill_sponsors(
    sponsor_id integer,
    bill_id integer,
    party_id smallint,
    role varchar,
    sponsor_type smallint,
    sponsor_start_date date,
    sponsor_end_date date,
    primary key (sponsor_id, bill_id)
);

--bill_votes table
DROP TABLE IF EXISTS clean.bill_votes;

CREATE TABLE clean.bill_votes(
    vote_id integer,
    bill_id integer,
    vote_date date,
    description varchar,
    yea smallint,
    nay smallint,
    nv smallint,
    absent smallint,
    total smallint,
    passed boolean,
    chamber varchar,
    url text,
    state_link text,
    primary key (vote_id, bill_id)
);

--bills table
DROP TABLE IF EXISTS clean.bills;

CREATE TABLE clean.bills(
    bill_id integer,
    session_id integer,
    bill_type varchar,
    bill_number varchar,
    state varchar,
    title varchar,
    description varchar,
    subjects text,
    introduced_date date,
    introduced_body varchar,
    url text,
    state_link text,
    primary key (bill_id, session_id)
);

--session_people table
DROP TABLE IF EXISTS clean.session_people;

CREATE TABLE clean.session_people(
    session_id integer,
    people_id integer,
    person_hash varchar,
    state_id smallint,
    party_id smallint,
    party varchar,
    role_id smallint,
    role varchar,
    name varchar,
    first_name varchar,
    last_name varchar,
    middle_name varchar,
    suffix varchar,
    nickname varchar,
    district varchar,
    ftm_id varchar,
    votesmart_id varchar,
    opensecrets_id varchar,
    ballotpedia varchar,
    committee_sponsor boolean,
    committee_id smallint,
    primary key (session_id, people_id)
);

--sessions table
DROP TABLE IF EXISTS clean.sessions;

CREATE TABLE clean.sessions(
    session_id integer,
    session_title varchar,
    year_start smallint,
    year_end smallint,
    state_id smallint,
    special boolean,
    primary key (state_id, session_id)
);

--bill_id_mapping
CREATE SEQUENCE IF NOT EXISTS clean.bill_id_mapping_entity_id_seq;

DROP TABLE IF EXISTS clean.bill_id_mapping;

CREATE TABLE clean.bill_id_mapping(
  entity_id serial,
  bill_id integer
);



