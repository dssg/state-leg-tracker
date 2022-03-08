#!/bin/bash

# credentials should be in ~/.pg_service.conf, just need to pass the name
# of the profile to look for: i.e. "$ sh create_raw_tables.sh lmillan"
psql -c "\i ../sql/create_clean_tables.sql" service=$1
