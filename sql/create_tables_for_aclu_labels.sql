-- Table for storing the labels received from ACLU

CREATE SCHEMA IF NOT EXISTS aclu_labels;

CREATE TABLE aclu_labels.issue_areas (
    doc_id integer,
    labeler varchar,
    labeling_date timestamp,  -- This contains the date that we sent them the file (The date appended to the filename), not necessarily when they labeled
    criminal_justice varchar,
    voting_rights varchar,
    racial_justice varchar,
    immigrants_rights varchar,
    tech_privacy_civil_liberties varchar,
    lgbtq_rights varchar,
    other varchar,
    notes text
);