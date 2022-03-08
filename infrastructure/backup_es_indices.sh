#!/bin/bash

echo "Starting the backup of ES indices..."
# persist actual es indices into docker volume
docker exec -it elastic_search sh -c "cp -R data es_indices/"

# put in /mnt/data the indices of that day
cp -r /home/$USER/es_indices /mnt/data/projects/aclu_leg_tracker/

echo "... backup complete, have a nice day!"
