#!/bin/bash

# DUMP="/mnt/data/projects/aclu_leg_tracker/legiscan_dump/dl.legiscan.com/snapshot-8iuz0322/*.zip"
# UNZIPPED="/mnt/data/projects/aclu_leg_tracker/legiscan_dump_20200615_processed/"

# mkdir -p $UNZIPPED

# for f in $DUMP
# do     
#     echo $unzip_target
#     unzip $f -d $UNZIPPED
# done

# The second dump
DUMP="/mnt/data/projects/aclu_leg_tracker/legiscan_dump_20210709/dl.legiscan.com/snapshot-8iuz0322/*.zip"
UNZIPPED="/mnt/data/projects/aclu_leg_tracker/legiscan_dump_20210709_processed/"

mkdir -p $UNZIPPED

for f in $DUMP
do     
    echo $unzip_target
    unzip $f -d $UNZIPPED
done
