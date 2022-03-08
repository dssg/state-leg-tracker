#!/bin/bash

echo "Start restoring ElasticSearch indices..."
# 1. copy stored ES indices
cp -r /mnt/data/projects/aclu_leg_tracker/es_indices /home/$USER/

# 2. run docker container in background mode
docker run -it -d --name elastic_search -v /home/$USER/es_indices:/usr/share/elasticsearch/es_indices -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch:7.7.1

# 3. restore indices
## rename original data folder
docker exec -it elastic_search sh -c "mv data old_data"

# move previous stored ES indices as new data folder
docker exec -it elastic_search sh -c "cp -r es_indices/data ."

# change owner to new data folder
docker exec -it elastic_search sh -c "chown -R elasticsearch data"

# change group to new data folder
docker exec -it elastic_search sh -c "chgrp -R root data"

# remove original data folder
docker exec -it elastic_search sh -c "rm -f -r old_data"

# 4. restart docker
docker restart elastic_search
echo "... Indices restored. Have a nice day!"
