-- setting role
SET role rg_staff;

-- Cleaning
DROP SCHEMA IF EXISTS catalogs CASCADE;

-- Bill types
DROP TABLE IF EXISTS catalogs.bill_types;

create table catalogs.bill_types (
	bill_type_id smallint,
	bill_type varchar,
	description varchar
)

-- Party
DROP TABLE IF EXISTS catalogs.political_party;

create table catalogs.political_party (
	party_id smallint,
	party_name varchar
)

-- Roles
DROP TABLE IF EXISTS catalogs.roles;

create table catalogs.roles (
	role_id smallint,
	role_name varchar
)

-- Sponsor types
DROP TABLE IF EXISTS catalogs.sponsor_types;

create table catalogs.sponsor_types (
	sponsor_type_id smallint,
	sponsor_type varchar
)

-- bill_status
DROP TABLE IF EXISTS catalogs.bill_status;

create table catalogs.bill_status (
	status_id smallint,
	status varchar,
	notes text default ''
)

-- supplement types
DROP TABLE IF EXISTS catalogs.supplement_types;

create table catalogs.supplement_types (
	supplement_type_id smallint,
	supplement_type varchar
)

-- Text types
DROP TABLE IF EXISTS catalogs.bill_text_types;

create table catalogs.bill_text_types(
	text_type_id smallint,
	text_type varchar
)

-- votes
DROP TABLE IF EXISTS catalogs.votes;

create table catalogs.vote_types (
	vote_type_id smallint,
	vote_type varchar
)

-- States
DROP TABLE IF EXISTS catalogs.states;

CREATE TABLE catalogs.states(
  state_id smallint,
  state_abbreviation varchar,
  state varchar,
  latitude float, -- Adding lat and long values for mapping in the UIs
  longitude float 
);










