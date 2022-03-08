import psycopg2
import logging
import uuid
import boto3

from typing import List
from psycopg2.extensions import connection

from src.etl.legiscan_interface import (
    get_state_sessions, 
    get_bills_in_session, 
    get_bill_info, 
    get_available_datasets, 
    get_dataset_content
)
from src.utils import project_constants as constants

def populate_sessions(db_conn: connection, api_key:str, states: List):
    """ Fetch and populate the sessions table in the raw schema"""
    
    cursor = db_conn.cursor()
    for state in states:
        logging.debug('Fetching for state: {}'.format(state))
        sessions = get_state_sessions(api_key=api_key, state=state)
        
        logging.info('Writing session info for state {}. Contains {} sessions '.format(state, len(sessions)))

        # TODO: Add the check of existence with session id and hash
        for sess in sessions:
            logging.info('Writing session info for {}'.format(sess['session_id']))
            q = "insert into raw.sessions VALUES ({});".format(', '.join(['%s'] * 6))

            var = (
                sess['session_id'], 
                sess['name'], 
                sess['year_start'], 
                sess['year_end'], 
                sess['state_id'],
                sess['session_hash']
            )
            
            try: 
                cursor.execute(q, var)
                db_conn.commit()  
            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(error)

    cursor.close()


def populate_bills(db_conn: connection, api_key: str, session_ids: List[int]):
    """ Populate the bills, bill sponsors and bill events tables for sessions """

    cursor = db_conn.cursor()

    for sess_id in session_ids:
        bills = get_bills_in_session(api_key=api_key, session_id=sess_id)
        bids = [x.get('bill_id') for x in bills.values()]

        logging.info('Writing info from session {}. Contains {} bills'.format(sess_id, len(bids)))
        
        for bid in bids:
            logging.info('Processing bill id: {} from session: {}'.format(bid, sess_id))

            bill_info = get_bill_info(api_key, bid)
          
            q_bills = "insert into raw.bills VALUES ({});".format(', '.join(['%s'] * 12))
            var_bills = (
                bill_info['bill_id'],
                sess_id,
                bill_info['bill_number'],
                bill_info['bill_type'],
                bill_info['change_hash'],
                bill_info['url'],
                bill_info['completed'],
                bill_info['state_id'],
                bill_info['status_date'],
                bill_info['status'],
                bill_info['title'],
                bill_info['description'],
            )

            # TODO: Improve performance by limiting server back and forth 
            try: 
                logging.info('Writing bill information')
                cursor.execute(q_bills, var_bills)
                db_conn.commit()  
            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(error)

            # Populating bill sponsors
            logging.info('Writing sponsor info')
            sponsors = bill_info['sponsors']
            for sponsor in sponsors:
                q_sponsors = "INSERT INTO raw.bill_sponsors VALUES ({});".format(', '.join(['%s'] * 7))
                
                var_sponsors = (
                    sponsor['people_id'],
                    bid,
                    sponsor['party'],
                    sponsor['role'],
                    sponsor['name'],
                    sponsor['district'],
                    sponsor['sponsor_type_id']                    
                )

                # TODO: Improve performance by limiting server back and forth 
                try: 
                    cursor.execute(q_sponsors, var_sponsors)
                    db_conn.commit()  
                except (Exception, psycopg2.DatabaseError) as error:
                    logging.error(error)

            # populating bill events
            logging.info('Writing bill event history')
            events = bill_info['history']
            for event in events:
                q_events = "INSERT INTO raw.bill_events VALUES ({});".format(', '.join(['%s']*4))
                var_events = (
                    bid,
                    event['date'],
                    event['action'],
                    event['chamber']
                )
                # TODO: Improve performance by limiting server back and forth 
                try: 
                    cursor.execute(q_events, var_events)
                    db_conn.commit()  
                except (Exception, psycopg2.DatabaseError) as error:
                    logging.error(error)

            # populating bill_votes
            logging.info('Writing bill voting info')
            votes = bill_info['votes']
            for roll_call in votes:
                q_votes = "INSERT INTO raw.bill_votes VALUES ({});".format(', '.join(['%s']*11))
                var_votes = (
                    roll_call['roll_call_id'],
                    bid,
                    roll_call['date'],
                    roll_call['desc'],
                    roll_call['yea'],
                    roll_call['nay'],
                    roll_call['nv'],
                    roll_call['absent'],
                    roll_call['total'],
                    roll_call['passed'],
                    roll_call['chamber']                    
                )
                # TODO: Improve performance by limiting server back and forth 
                try: 
                    cursor.execute(q_votes, var_votes)
                    db_conn.commit()  
                except (Exception, psycopg2.DatabaseError) as error:
                    logging.error(error)
                    
    cursor.close() 

def populate_datasets(db_conn: connection, api_key: str, s3_creds):
    """
        Fetch all available Datasets from the API and populates the raw.datasets table. 
        Additionally, the encoded dataset content is stored in S3
    """
    datasets = get_available_datasets(api_key)

    for ds in datasets:
        logging.info('Storing dataset for session {}'.format(ds['session_id']))
        content, mime_type = get_dataset_content(api_key, ds['session_id'], ds['access_key'])

        if mime_type != "application/zip":
            logging.warning('MIME type is not Zip for session {} dataset'.format(ds['session_id']))

        content_uuid = uuid.uuid4().hex
        logging.debug(content_uuid)


        date_formatted = ds['dataset_date'].replace('-','')

        # S3 bucket path
        s3_bucket = constants.S3_BUCKET
        folder = 'raw/{}/{}'.format(ds['session_id'], date_formatted) 
        s3_path = 's3://{}/{}'.format(s3_bucket, folder)
        s3_file_key = '{}/{}'.format(folder, content_uuid)

        # Write the content to S3
        session = boto3.Session(
            aws_access_key_id=s3_creds['aws_access_key_id'],
            aws_secret_access_key=s3_creds['aws_secret_access_key']
        )
        
        logging.info('Storing encoded content in S3 at {}'.format(s3_path))
        s3 = session.resource('s3')
        s3.Bucket(s3_bucket).put_object(Key=s3_file_key, Body=content)
        
        # SQL query
        q = "INSERT INTO raw.datasets VALUES ({})".format(', '.join(['%s']*6))
        var = (
            ds['dataset_hash'],
            ds['dataset_date'],
            ds['dataset_size'],
            ds['session_id'],
            s3_path,
            content_uuid,
        )
        
        # Writing database entry
        cursor = db_conn.cursor()
        try: 
            logging.info('writing dataset entry to the database')
            cursor.execute(q, var)
            db_conn.commit()  
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)

        cursor.close()
