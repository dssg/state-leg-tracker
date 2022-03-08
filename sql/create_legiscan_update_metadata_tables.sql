set role rg_staff;

DROP SCHEMA IF EXISTS legiscan_update_metadata;

CREATE SCHEMA legiscan_update_metadata;

--bill hashes
DROP TABLE IF EXISTS legiscan_update_metadata.bill_hashes;

CREATE TABLE legiscan_update_metadata.bill_hashes(
    bill_id varchar,
    session_id varchar,
    bill_hash varchar,
    update_date date
);

--session hashes
DROP TABLE IF EXISTS legiscan_update_metadata.session_hashes;

CREATE TABLE legiscan_update_metadata.session_hashes(
    session_id varchar,
    state_id varchar,
    session_hash varchar,
    update_date date
);