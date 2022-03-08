#!/bin/bash


# credentials should be in ~/.pg_service.conf, just need to pass the name
# of the profile to look for: i.e. "$ sh populate_clean_tables_from_sh.sh aclu"
psql -c "\i ../sql/populate_from_es_to_postgres_clean.sql" service=$1
