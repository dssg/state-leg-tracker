### Foreign Data Wrapper

In order to sync ElasticSearch with Postgres we will use a Foreign Data Wrapper from Postgres that will expose the ElasticSearch data as a foreign data table in Postgres.

#### Installation of the foreign data wrapper in RDS EC2 machine

1. In the RDS EC2 machine install the following packages:

The version of ElasticSearch we are using:

```
sudo pip install "elasticsearch>=7,<8"
```

|Postgres version|Corresponding package|
|:------|:-------------------------------------------|
|10.x|`sudo apt-get install postgresql-10-python-multicorn`|
|11.x|`sudo apt-get install postgresql-11-python-multicorn`|
|12.x|`sudo apt-get install postgresql-12-python3-multicorn`|

And the foreign data wrapper for ElasticSearch:

```
sudo pip install pg_es_fdw
```

2. Restart Postgres service:

```
service postgresql stop
service postgresql start
```

3. Within the database `aclu_leg_tracker`

Enable the multicorn extension:

```
CREATE EXTENSION multicorn;
```

Create a server for the ElasticSearch communication:

```
CREATE SERVER multicorn_es FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'pg_es_fdw.ElasticsearchFDW',
  host 'localhost',
  port '9200',
  rowid_column 'id',
  query_column 'query',
  score_column 'score',
  timeout '20',
  username 'elastic',
  password ''
);
```

If there is no error message then the FDW extension for ElasticSearch has been successfully set up.

After the FDW for ElasticSearch has been enable, we require to define the foreign tables associated to each index on ElasticSearch.

For complete documentation [Matthew Franglen Github repo](https://github.com/matthewfranglen/postgres-elasticsearch-fdw)
