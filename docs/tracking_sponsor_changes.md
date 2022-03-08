## Tracking Changes in Bill Sponsors

The list of co-sponsors of a bill can change over time, and could contain important information. LegiScan does not keep track of when a sponsor was added to the bill. 
As of the time of writing (2021-11-10), our models assume that all the sponsors are added at the time of introduction.
This could be a source of data leakage as the average success rate of the bill sponsors is a highly predictive feature in our models. 

Even though LegiScan does not keep track of the sponsor changes, we can at least approximate the sponsored date during our weekly data update. 
So, we have setup the weekly data update pipeline (`src/pipelines/legiscan_updates.py`) to perform the following:
- For each new bill, add the attribute `sponsored_date` and set it to the `introduced_date` for each sponsor in the list of sponsors
- For each updated bill (a bill with an upadated hash), 
    - we compare the two sponsor lists and to each new sponsor add the `sponsored_date` field and set it to the date on which the pipeline is run
    - In addidion, if the `sponsored_date` field does not exist for the other sponsors of the bill, we add it and set it to the introduction date


This leaves us with some bills that contains the field `sponsored_date` and some without. To fix this, we run a small script (`src/etl/initialize_sponsore_date_es.py`) that initializes the `sponsored_date` field to the introduction date for every bill in the ES index.  
