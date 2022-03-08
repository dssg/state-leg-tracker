import sys
import logging
import base64
import boto3
import json
import numpy as np
import multiprocessing as mp
import time

from multiprocessing import Process, Queue, cpu_count

from src.utils import project_constants as constants
from src.etl.bill_doc_creator import parse_session_zip_file
from src.utils.general import get_s3_credentials


def parse_legiscan_dump(s3_creds, s3_source_folder, s3_target_folder, n_jobs=-1):
    """ parse the dataset dump given by Legiscan

        Parses the zip files in the dataset for each session and writes a json for each bill to S3. 
        These json files can be loaded to elastic search 

        Args:
            s3_creds: credentials for S3
            s3_source_folder: folder in S3 that contains the zip files
            s3_target_folder: foldser where the bill docs will be saved
            n_jobs: number of processes to parallelize the job over, defaults to -1 (using all available processing power)
    """

    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )
    s3 = session.resource('s3')
    s3_bucket = constants.S3_BUCKET

    # The list of all zip files
    zip_objects_all = s3.Bucket(s3_bucket).objects.filter(Prefix=s3_source_folder)

    logging.info('Already extracted sessions')
    already_extracted = s3.Bucket(s3_bucket).objects.filter(Prefix=s3_target_folder)
    extracted_sessions = np.array([x.key.split('/')[1].split('_')[0] for x in already_extracted])
    extracted_sessions = np.unique(extracted_sessions)

    # THe session id is enclosed in square brackets in the key name
    # Using that to filter the sessions that were already extracted and decoded
    zip_objects = list()
    ct = 0
    logging.info('Identifying the sessions to be extracted')
    for i, x in enumerate(zip_objects_all):
        if i==0:
            continue

        try:
            session_num = x.key.split('[')[1].split(']')[0]
            if session_num not in extracted_sessions:
                zip_objects.append(x)
        except:
            logging.warning('No session number in file name')
            continue
        
        ct = i

    n_files = len(zip_objects)

    logging.info('Total number of sessions: {}'.format(ct))
    logging.info('Already extracted sessions: {}'.format(len(extracted_sessions)))
    logging.info('To be extracted sessions: {}'.format(n_files))

    if n_jobs == -1:
        n_jobs = cpu_count()

    # Jobs are equally distributed across processes. 
    # In case that the files are not divisible by the number of jobs, 
    # distributing the remainder across the processes one by one
    chunk_size = (n_files//n_jobs)
    chunks_list = [chunk_size] * n_jobs
    remainder = n_files - (chunk_size * n_jobs)

    for i in range(remainder):
        chunks_list[i] = chunks_list[i] + 1    

    logging.info('chunk sizes {}'.format(chunks_list))

    jobs = list()
    for i in range(n_jobs):
        chunk_size = chunks_list[i]
        s = i * chunk_size
        e = min(n_files, s + chunk_size)
        logging.info('s: {}, e: {}'.format(s, e))

        # last iteration and not reached all the files
        if (i==n_jobs-1) and (e < n_files): 
            e = n_files

        p = Process(
            name='p'+str(i),
            target=_parse_subprocess_target,
            kwargs={
                'zip_files': zip_objects[s:e],
                's3': s3,
                's3_folder': s3_target_folder,
                # 'results_queue': results
            }
        )

        jobs.append(p)
        p.start()

    for p in jobs:
        p.join()
        # p.terminate()
        # p.join()


def _parse_subprocess_target(zip_files, s3, s3_folder):
    """ the target function for a process. Parses the set of files assigned to the process"""
    
    # multiprocessing log
    pname = mp.current_process().name
    logging.info('Process {}: Processing {} files'.format(pname, len(zip_files)))

    s3_bucket = constants.S3_BUCKET
    bill_counter = 0
    session_counter = 0
    for session_zip in zip_files:
        logging.info('{}: Processing bills of session: {}'.format(pname, session_zip.key))
        zip_content = session_zip.get()['Body'].read()

        if zip_content:
            logging.info(f'{pname}: parsing bills')
            # bills_list = parse_session_zip_file(zip_content)
            parse_session_zip_file(zip_content, s3, s3_folder)
            
            # logging.info(f'{pname}: Writing bill documents to S3')
            # for bill in bills_list:
            #     fn = '{}_{}.json'.format(bill['session_id'], bill['bill_id'])
            #     fkey = '{}/{}'.format(s3_folder, fn)

            #     s3.Bucket(s3_bucket).put_object(Key=fkey, Body=json.dumps(bill))
            #     bill_counter+=1
    
            # logging.info(f'{pname}: Finished writing to S3')

        session_counter += 1
        logging.info(f'{pname}: Finished session {session_zip.key}')

    logging.info('{}: Finished process, wrote {} bills in {} sessions'.format(
            pname,
            bill_counter,
            session_counter
        )
    )
