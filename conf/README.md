### Project Credentials

This folder should contain credentials required for accessing the data in the project within a `credentials.yaml` file. This file should reside only on your local machine, **DO NOT** commit the file to the repo. The provided `credentials_example.yaml` file shows how the credentials should be formatted. 

You would need the following credentials

**Database** – Credentials to the database that stores the legislative bill metadata

**Elasticsearch** – Credentials to the elasticsearch cluster that stores the bill text

**Legiscan** – An API key for LegiScan to keep the dataset refreshed every week

**AWS** – [Optional] Aws keys for accessing S3 buckets used for storing intermediate data snapshots and `triage` models & matrices. Alternatively, these can be setup in your home directory `.aws` folder. 
