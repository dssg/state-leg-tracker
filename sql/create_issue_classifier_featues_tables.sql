set role rg_staff;

--schema
DROP SCHEMA IF EXISTS issue_classifier_features CASCADE;

CREATE SCHEMA issue_classifier_features;

--table
DROP TABLE IF EXISTS issue_classifier_features.features;

CREATE TABLE issue_classifier_features.features(
    as_of_date timestamp without time zone,
    entity_id integer,
    reproductive_rights_label smallint,
    criminal_law_reform_label smallint,
    immigrant_rights_label smallint,
    lgbt_rights_label smallint,
    racial_justice_label smallint,
    voting_rights_label smallint,
    features jsonb
);