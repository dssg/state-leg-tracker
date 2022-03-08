#!/bin/sh

# Shell script to run the legiscan data dump parser

# credentials file
cred_file_path=../../conf/local/credentials.yaml

# source folder in the s3 bucket (folder with the legiscan dump)
source=legiscan_dump_20200615

# target folder in the s3 bucket (folder where the bill docs will be placed)
target=extracted_bill_docs_parallelized

# number of parallel processes (-1 for using all available)
n_jobs=-1

python legiscan_dump_parser.py $cred_file_path $source $target $n_jobs