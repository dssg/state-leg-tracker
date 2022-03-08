import os
import sys
import logging
import boto3
import json
import logging
import tempfile
import numpy as np
import multiprocessing as mp

from datetime import datetime
from multiprocessing import Process, Queue, cpu_count
from zipfile import ZipFile

from src.utils import project_constants as constants
from src.utils.general import get_s3_credentials

timestr = datetime.now().strftime("%y%m%d%H%M%S")
logging.basicConfig(level=logging.INFO, filename=f"../../logs/legiscan_dump_people_decoder_{timestr}.DEBUG", filemode='w')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))


# TODO: to be merged with the parse_legiscan_dump() in legiscan_dump_parser
def parse_people_from_dump(s3_session: boto3.Session, s3_source, s3_target, n_jobs=1):
    """ Parse the legiscan dump to extract people information """

    s3 = s3_session.resource('s3')
    s3_bucket = constants.S3_BUCKET

    # all zip files
    all_zip_objects = s3.Bucket(s3_bucket).objects.filter(Prefix=s3_source)

    # Already extracted files
    # This is to avoid redoing work incase of interruption
    logging.info('Already extracted sessions')
    already_extracted = s3.Bucket(s3_bucket).objects.filter(Prefix=s3_target)

    # The zip files take this form <ST>_<start_year>_<end_year>_<name>_[<id>].zip 
    # e.g. AK_2011-2012_27th_Legislature_[122].zip
    extracted_sessions = np.array([x.key.split('/')[1].split('_')[0] for x in already_extracted])
    extracted_sessions = np.unique(extracted_sessions)
    
    # A list of dictionaries holds the file objets that need to be extracted
    # Dictionary contains the session_id, and the zip object
    to_unzip = list()
    
    logging.info('Checking for already extracted sessions')
    session_count = 0 # Keeping track of the number of sessions for debugging purposes
    for i, x in enumerate(all_zip_objects):
        if i == 0: # The first element is the folder name
            continue

        try:
            # The session id is enclosed in square brackets in the key name
            session_num = x.key.split('[')[1].split(']')[0]
            if session_num not in extracted_sessions:
                d = dict()
                d['session_id'] = session_num
                d['zip_object'] = x
                to_unzip.append(d)
            
            # Keeping track of the sessions
            session_count += 1

        except:
            logging.warning('No session number in the file name {}. Skipping'.format(x))
            continue

    n_files_to_unzip = len(to_unzip)

    logging.info('Total session count: {}'.format(session_count))
    logging.info('Already extracted: {}'.format(len(extracted_sessions)))
    logging.info('To be extracted: {}'.format(n_files_to_unzip))

    if n_jobs == -1:
        n_jobs = cpu_count()
    
    # Jobs are equally distributed across processes. 
    # In case that the files are not divisible by the number of jobs, 
    # distributing the remainder across the processes one by one
    chunk_size = (n_files_to_unzip//n_jobs)
    chunks_list = [chunk_size] * n_jobs
    remainder = n_files_to_unzip - (chunk_size * n_jobs)

    # Distributing the remainder
    for i in range(remainder):
        chunks_list[i] = chunks_list[i] + 1 

    logging.info('Chunk sizes {}'.format(chunks_list))
    jobs = list()
    for i in range(n_jobs):
        chunk_size = chunks_list[i]

        start_idx = i * chunk_size
        end_idx = min(n_files_to_unzip, start_idx + chunk_size)

        logging.info('Start:{}, End: {}'.format(start_idx, end_idx))

        # In the last iteration, checking whether all the files are caught
        if (i==n_jobs-1) and (end_idx < n_files_to_unzip):
            end_idx = n_files_to_unzip

        p = Process(
            name='p{}'.format(i),
            target=_subprocess_target,
            kwargs={
                's3_session': s3_session,
                'zip_files': to_unzip[start_idx:end_idx],
                's3_target': s3_target
            }
        )

        jobs.append(p)
        p.start()
    
    for p in jobs:
        p.join()


def _subprocess_target(s3_session, zip_files, s3_target):
    """ Parsing the files assigned to the process 
        Args:
            s3_session: boto3 session
            zip_files: A list of dictionaries with the session_id and S3 objects for session zip files
            s3_target: The target folder in the S3 bucket
    """
    # multiprocessing log
    pname = mp.current_process().name
    logging.info('Process {}: Processing {} files'.format(pname, len(zip_files)))

    for session_zip in zip_files:
        session_id = session_zip['session_id']
        zip_obj = session_zip['zip_object']

        zip_content = zip_obj.get()['Body'].read()

        _parse_people_from_zip(s3_session, session_id, zip_content, s3_target)


def _parse_people_from_zip(s3_session, session_id, zip_file_content, s3_target):
    """ Given a zip file parse the people information and write to S3 
        Args:
            s3_session: A boto3 session
            zip_file_content: Decoded zip file content
            s3_target: Target folder in the S3 bucket
    """

    current_process_name = mp.current_process().name

    logging.info('{}: Parsing session {}'.format(current_process_name, session_id))
    
    s3_bucket = constants.S3_BUCKET
    s3 = s3_session.resource('s3')
    
    fp = tempfile.TemporaryFile()
    fp.write(zip_file_content)

    zip_ref = ZipFile(fp, 'r')

    for fname in zip_ref.namelist():
        content = zip_ref.read(fname)
        content = json.loads(content)
        
                
        if 'people' in fname:
            person_contens = content.get('person')
            
            # Adding the session id to the json
            person_contens['session_id'] = session_id
            
            # The legiscan people id
            people_id = person_contens.get('people_id')
            
            # Writing to S3 
            logging.info('{}: writing person id {} of session {} to S3'.format(
                current_process_name,
                people_id,
                session_id
            ))

            fn = '{}_{}.json'.format(session_id, people_id)
            fkey = '{}/{}'.format(s3_target, fn)
            s3.Bucket(s3_bucket).put_object(Key=fkey, Body=json.dumps(person_contens))  


def main():
    try:
        n_jobs = sys.argv[1]
    except:
        n_jobs = -1

    creds_folder = '../../conf/local/'
    fpath = os.path.join(creds_folder, 'credentials.yaml')

    s3_creds = get_s3_credentials(fpath)
    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )

    parse_people_from_dump(
        s3_session=session,
        s3_source='legiscan_dump_20200615',
        s3_target='extracted_session_people',
        n_jobs=n_jobs
    )


if __name__ == '__main__':
    main()


