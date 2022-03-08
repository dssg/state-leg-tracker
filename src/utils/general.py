import os
import logging
import yaml
import psycopg2
import sqlalchemy
import boto3
import pandas as pd
import joblib

from io import BytesIO, StringIO
from elasticsearch import Elasticsearch
# from elastic_app_search import Client

import src.utils.project_constants as constants 


def get_legiscan_key(creds_file):
    """get the legiscan API key"""
    creds = read_yaml_file(creds_file)
    key = creds['legiscan']['api_key']

    return key


def get_s3_credentials(creds_file):
    """ Get credentials for accessing AWS S3 buckets from the credentials file"""
    creds = read_yaml_file(creds_file)

    s3_creds = creds['s3']

    return s3_creds

def get_boto3_session(creds_file):
    """ Get a boto3 session for """
    s3_credentials = read_yaml_file(creds_file)['s3']
    
    s3_session = boto3.Session(
        aws_access_key_id=s3_credentials['aws_access_key_id'],
        aws_secret_access_key=s3_credentials['aws_secret_access_key']
    )

    return s3_session   

def read_yaml_file(yaml_file):
    """ load yaml cofigurations """

    config = None
    try: 
        with open(yaml_file, 'r') as f:
            config = yaml.safe_load(f)
    except:
        raise FileNotFoundError('Couldnt load the file')
    
    return config


def get_db_conn(creds_file, conn_type='psycopg2'):
    """ Get an authenticated db connection, given a credentials file
        The db connection type defaults to psycopg2, but can be modified to return a sqlalchemy engine
        For triage, sqlalchemy engines are useful
    """
    creds = read_yaml_file(creds_file)['db']

    if conn_type=='psycopg2':
        connection = psycopg2.connect(
            user=creds['user'],
            password=creds['pass'],
            host=creds['host'],
            port=creds['port'],
            database=creds['db']
        )
    else:
        poolclass=sqlalchemy.pool.QueuePool
        dburl = sqlalchemy.engine.url.URL(
            "postgres",
            host=creds["host"],
            username=creds["user"],
            database=creds["db"],
            password=creds["pass"],
            port=creds["port"],
        )

        connection = sqlalchemy.create_engine(dburl, poolclass=poolclass)

    return connection


def get_elasticsearch_conn(creds_file):
    """ Get an elasticsearch object from the elasticsearch cluster"""
    creds = read_yaml_file(creds_file)['es']

    return Elasticsearch([{'host': creds['host'], 'port': creds['port']}], timeout=30, max_retries=10, retry_on_timeout=True)


# def get_elastic_app_search_client(creds_file):
#     """Get an app search client"""
#     creds = read_yaml_file(creds_file)['elastic_app_search']

#     client = Client(
#         base_endpoint=creds['base_endpoint'],
#         api_key=creds['api_key'],
#         use_https=False
#     )

#     return client


def get_issue_area_configuration(conf_file):
    """Get the configuration for issue area classifier"""
    issue_area_conf = read_yaml_file(conf_file)

    return issue_area_conf


def format_s3_path(s3_path):
    """ This function removed the S3://<s3_bucket> from an S3 path and return the file key
        useful for using with Boto3
    """

    fkey = s3_path.replace('s3://{}/'.format(constants.S3_BUCKET), '')

    return fkey


def load_matrix_s3(s3_session, matrix_path, compression=None):
    """ load a triage matrix stored in an S3 bucket """

    s3_bucket = constants.S3_BUCKET
    s3 = s3_session.resource('s3')

    # Stripping the s3://<s3_bucket>/ 
    fkey = format_s3_path(matrix_path)

    content = s3.Object(s3_bucket, fkey).get()['Body'].read()
    matrix = pd.read_csv(BytesIO(content), compression=compression)

    matrix = matrix.set_index(['entity_id', 'as_of_date'])

    return matrix


def load_model_s3(s3_session, model_path):
    """" Load a pikled model stored in an S3 bucket """
    s3_bucket = constants.S3_BUCKET
    s3 = s3_session.resource('s3')

    # Stripping the s3://<s3_bucket>/ 
    fkey = format_s3_path(model_path)

    content = s3.Object(s3_bucket, fkey).get()['Body'].read()
    mod_obj = joblib.load(BytesIO(content))

    return mod_obj


def copy_df_to_pg(engine, table_name, df, columns_to_write=None):
    """ Write a dataframe to postgres table using the psycopg copy_from function

        args:
            engine: Psycopg2 engine,
            table_name: The table to write
            df: The dataframe. Should have the appropriate column data types. For instance, pandas converts numeric columns to floats by default. 
                Need to make sure that integer columns are integer. The column names should match the pg table
            columns_to_write: If selecting a subset of columns. default None (write all columns in df)
    """
    
    if columns_to_write is None:
        columns_to_write = list(df.columns)

    csv_buffer = StringIO()
    df[columns_to_write].to_csv(csv_buffer, index=False, header=False, sep='\t')
    csv_buffer.seek(0)

    cursor = engine.cursor()

    try:
        cursor.copy_from(
            csv_buffer, 
            table_name, 
            sep='\t',
            columns=columns_to_write
        )
        engine.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        raise psycopg2.DatabaseError(error)

