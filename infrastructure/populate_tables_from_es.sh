#!/bin/bash


# credentials should be in ~/.pg_service.conf, just need to pass the name
# of the profile to look for: i.e. "$ sh populate_catalogues.sh lmillan"
psql -c "\i ../sql/populate_from_es_to_postgres.sql" service=$1
