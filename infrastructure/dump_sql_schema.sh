#!/bin/sh

#### Dumping the issue_classifier_results schema to make space
#### NOTE -- This uses postgres 12.3 so can't run on King-Friday
# echo "working on the dump"
# pg_dump -h ec2-34-220-100-126.us-west-2.compute.amazonaws.com -p 5432 --role rg_staff -d aclu_leg_tracker -n issue_classifier_results > /mnt/data/db_backups/aclu_issue_classifier_results_20210206.dmp

# echo "dump is complete. Copying to S3"
# aws s3 mv /mnt/data/db_backups/aclu_issue_classifier_results_20210206.dmp s3://aclu-leg-tracker/db_backups/20210206/

# echo "operation complete"


#### NOTE -- This uses postgres 12.3 so can't run on King-Friday
#### Dumping triage schemas (triage_metadata, train_results, and test_results) of all previous experiments to make space -- 2021.02.11
#### This is a very crude fix overcome space issues in the EC2 to run more passage experiments
#### Before we did the dump, renamed the schema -- ALTER SCHEMA <schema_name> RENAME TO <schema_name>_20210211;

# echo "working on the dump of triage_metadata"
# pg_dump -h ec2-34-220-100-126.us-west-2.compute.amazonaws.com -p 5432 --role rg_staff -d aclu_leg_tracker -n triage_metadata_20210211 > /mnt/data/db_backups/triage_metadata_20210211.dmp
# echo "triage_metadata dump is complete. Copying to S3"
# aws s3 mv /mnt/data/db_backups/triage_metadata_20210211.dmp s3://aclu-leg-tracker/db_backups/20210211_triage_backup/


echo "working on the dump of train_results"
pg_dump -h ec2-34-220-100-126.us-west-2.compute.amazonaws.com -p 5432 --role rg_staff -d aclu_leg_tracker -n train_results_20210211 > /mnt/data/db_backups/train_results_20210211.dmp
echo "train_results dump is complete. Copying to S3"
aws s3 mv /mnt/data/db_backups/train_results_20210211.dmp s3://aclu-leg-tracker/db_backups/20210211_triage_backup/


# echo "working on the dump of test_results"
# pg_dump -h ec2-34-220-100-126.us-west-2.compute.amazonaws.com -p 5432 --role rg_staff -d aclu_leg_tracker -n test_results_20210211 > /mnt/data/db_backups/test_results_20210211.dmp
# echo "test_results dump is complete. Copying to S3"
# aws s3 mv /mnt/data/db_backups/test_results_20210211.dmp s3://aclu-leg-tracker/db_backups/20210211_triage_backup/


echo "operation complete"

## copied donors2 s3://dsapp-cmu-research/BIAS/donors2/
## TODO -- add a backup.txt file to donors2

