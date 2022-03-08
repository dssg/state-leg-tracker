import psycopg2
import logging
import uuid
import boto3
import json
import base64
import tempfile
import requests

from zipfile import ZipFile
from typing import List
from psycopg2.extensions import connection

from src.utils import project_constants as constants
from src.utils.decoders import pdf_decoder

def extract_datasets_in_db(db_conn: connection, s3_creds):
    """ For the datasets in the raw.datasets table 
        extract the contents on the zip files  and store them on S3 """
    # TODO: Add an extracted column in the raw.datasets table and extract only the ones which are not
    cursor = db_conn.cursor()
    q = "select s3_path, uuid_value, state_id from raw.datasets left join raw.sessions using (session_id)"

    try:
        cursor.execute(q)
        results = cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        return

    cursor.close()

    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )
    s3 = session.resource('s3')
    s3_bucket = constants.S3_BUCKET

    # For each dataset in results
    for r in results:
        logging.info(r)

        # # TODO: This is to avoid re-running the states that were already extracted. Remove later
        # if r[2] in ['1', '2', '3', '4']:
        #     logging.info('already processed {}'.format(r[2]))
        #     continue

        
        s3_path = r[0]
        zip_uuid  = uuid.UUID(r[1]).hex
        logging.info('Extracting state: {}'.format(r[2]))
        logging.info('processing file at {}'.format(s3_path))

        # Removing the s3://bucket
        folder = s3_path.replace('s3://{}/'.format(s3_bucket), '')
        file_key = '{}/{}'.format(folder, zip_uuid)

        obj = s3.Object(s3_bucket, file_key)
        encoded_text  = obj.get()['Body'].read()
        decoded_text = base64.b64decode(encoded_text)

        fp = tempfile.TemporaryFile()
        fp.write(decoded_text)
        zip_ref = ZipFile(fp, 'r')

        for fname in zip_ref.namelist():
            # logging.info('extracting : {}'.format(fname))

            # stripping off the state and the session name
            # TODO: This is hacky. Figure out a way to properly do this if we contiue this route
            temp = fname.split('/')
            fkey = '{}/{}'.format(temp[-2], temp[-1])

            s3.meta.client.upload_fileobj(
                zip_ref.open(fname),
                Bucket=s3_bucket,
                Key='{}/extracted/{}'.format(folder, fkey)
            ) 

def add_bill_text_to_db_content(db_conn: connection, s3_creds):
    """Fetching and adding the bill pdf contents to the bill information"""
    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )
    s3 = session.resource('s3')

    cursor = db_conn.cursor()
    q = "select s3_path, uuid_value from raw.datasets"

    try:
        cursor.execute(q)
        results = cursor.fetchall()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        return

    cursor.close()

    for r in results:
        s3_path = r[0]
        logging.info('processing file at {}'.format(s3_path))

        folder = s3_path.replace('s3://{}/'.format(s3_bucket), '')
        bill_folder = '{}/{}'.format(folder, 'extracted/bill/')

        s3_bucket = s3.Bucket(constants.S3_BUCKET)
        for bill_obj in s3_bucket.objects.filter(Prefix=bill_folder):
            logging.info('processing {}'.format(bill_obj.key))

            # Bytes
            bill_content  = bill_obj.get()['Body'].read().decode()
            bill_info = json.loads(bill_content)

            # Extracting the content
            _extract_bill_content(bill_info, s3_creds)
            logging.info(bill_info.get('bill').get('texts'))

            # Writing the modified json file
            s3_bucket.put_object(Key=bill_obj.key, Body=json.dumps(bill_info))

        # logging.info(bill_json)        


def _extract_bill_content(bill_info, s3_creds):
    """Given a json of a bill, grab and decode the PDF of the bill
        Args:
            bill_info: bill information extracted from the dataset zip file in json format
        return:
            modified bill information. 
    """
    docs = bill_info.get('bill').get('texts')

    # One bill can have several docs
    for doc in docs:
        # TODO: Use the legiscan link not the statelink
        url = doc.get('state_link')
        if url.endswith('.pdf'):
            logging.info('decoding {}'.format(url))
            pdf_file = requests.get(url)
            decoded = pdf_decoder(pdf_file.content)
            doc['doc_text'] = decoded  
            
