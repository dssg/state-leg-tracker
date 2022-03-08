import sys
import psycopg2
import json
import base64
import tempfile
import logging
import pickle
import pandas as pd
import numpy as np
import itertools
import boto3

import src.utils.project_constants as constants
from src.utils.decoders import html_decoder, pdf_decoder

from urllib.request import urlopen
from zipfile import ZipFile
from datetime import date, timedelta

from src.utils.general import get_legiscan_key, get_db_conn, get_elasticsearch_conn, get_s3_credentials

API_KEY = get_legiscan_key('../../conf/local/credentials.yaml')
CURRENT_YEAR = date.today().year

# TODAY = '2021-08-11'
TODAY = date.today()

API_CALL_COUNTER = 0


def _track_sponsor_changes(es, bill_id, new_sponsors, bill_progress, event_history):
    """
        Compare the list of sponsors on ES and fetched to track the "sponsored_date" field
        Currently, the field will contain the closest pipeline run date 
        as we have no indication in the data about when the sponsor was added

        Args:
            es: Elasticsearch connection
            bill_id: ID of the bill,
            sponsors (List[dict]): The list of sponsors in the latest JSON
            bill_progress_events: The status changes of the bill. We use this to extract the introduced date. 
                We default the sponsored_date to the bill introduction date

        return:
            Modified sponsors list with the added /modified field "sponsored_date"
    """

    q = {'query': {'match': {'bill_id': bill_id}}}
    res = es.search(index=constants.BILL_META_INDEX, body=q, _source=["sponsors"])

    # We use the first date that appears
    try:
        intro_date = [x['date'] for x in bill_progress if x['event']==1][0]
    # There are a few cases where the introduced event is not present in data
    # But these bills are introduced and passes/dies on the same day. 
    # Therefore, has little bearing on the models we build
    # So, just filling in a date to keep structure consistent
    except IndexError:
        if not event_history:
            intro_date = '1970-01-01'
            logging.warning('No progress or event history found. Setting 1970-01-01 as the sponsor date')
        else:
            event_dates = [x['date'] for x in event_history]
            intro_date = sorted(event_dates)[0] 

    num_hits = res['hits']['total']['value']
    if num_hits == 0:
        logging.info(f'Bill ID {bill_id} is new. Introduction date adde as the sponsored_date')
        for sponsor in new_sponsors:
            sponsor['sponsor_start_date'] = intro_date
            sponsor['sponsor_end_date'] = 'null'

        return new_sponsors
    
    else:
        # We should only have one hit for our query
        curr_sponsors = res['hits']['hits'][0]['_source']['sponsors']

        # Indexing the list of sponsors by their ID helps us effectively search sponsors and changes
        # Otherwise we would have to do repeated searches over the list
        curr_indexed_by_id = {x['people_id']: x for x in curr_sponsors}
        new_indexed_by_id = {x['people_id']: x for x in new_sponsors}

        # We need to keep track of all sponsors (new, and ones who have dropped out)
        all_sponsors = {**curr_indexed_by_id, **new_indexed_by_id}

        # Newly added sponsors
        # New - current
        added = set(new_indexed_by_id.keys()).difference(set(curr_indexed_by_id.keys()))
        logging.info(f'{added} -- New set of sponsors')
 
        # dropped sponsores
        # Current - New
        dropped = set(curr_indexed_by_id.keys()).difference(set(new_indexed_by_id.keys()))
        logging.info(f'{dropped} -- Dropped set of sponsors')

        # For all new sponsors we update the start date
        for a in added:
            sponsor = all_sponsors[a]
            sponsor['sponsor_start_date'] = TODAY

        # For sponsors who dropped out, we update the end date
        for d in dropped:
            sponsor = all_sponsors[d]
            sponsor['sponsor_end_date'] = TODAY


        # TODO: We are not handling the edge case where the same sponsor drops out and joins back
        # We would like to keep both records for that sponsor

    
        return list(all_sponsors.values())



def _update_bill_metadata(response):
    """
    Updates the metadata of a bill on elasticsearch
    :param response (Dict): the content of the bill json loaded from the dataset2
    :return:
    """

    es = get_elasticsearch_conn("../../conf/local/credentials.yaml")

    # there are bills with amendments date = 0000-00-00
    amendments = response['bill']['amendments']
    if len(amendments) > 0:
        for element in amendments:
            if element['date'] == '0000-00-00':
                element['date'] = '1970-01-01'

    votes = response['bill']['votes']
    if len(votes) > 0:
        for element in votes:
            if element['date'] == '0000-00-00':
                element['date'] = '1970-01-01'

    if len(response['bill']['texts']) > 0:
        for element in response['bill']['texts']:
            if element['date'] == '0000-00-00':
                element['date'] = '1970-01-01'

    logging.info('Checking for any changes in sponsors, and adding the sponsored date')

    updated_sponsors = _track_sponsor_changes(
        es=es,
        bill_id=response['bill']['bill_id'],
        new_sponsors=response['bill']['sponsors'],
        bill_progress=response['bill']['progress'],
        event_history=response['bill']['history']
    )
    
    body = {
        'bill_id': response['bill']['bill_id'],
        'bill_number': response['bill']['bill_number'],
        'bill_type': response['bill']['bill_type'],
        'bill_type_id': response['bill']['bill_type_id'],
        'amendments': response['bill']['amendments'],
        'body': response['bill']['body'],
        'body_id': response['bill']['body_id'],
        'change_hash': response['bill']['change_hash'],
        'committee': response['bill']['committee'],
        'current_body': response['bill']['current_body'],
        'current_body_id': response['bill']['current_body_id'],
        'description': response['bill']['description'],
        'history': response['bill']['history'],
        'progress': response['bill']['progress'],
        'session': response['bill']['session'],
        'sponsors': updated_sponsors,
        'state': response['bill']['state'],
        'state_id': response['bill']['state_id'],
        'state_link': response['bill']['state_link'],
        'status': response['bill']['status'],
        'status_date': response['bill']['status_date'],
        'subjects': response['bill']['subjects'],
        'title': response['bill']['title'],
        'url': response['bill']['url'],
        'votes': response['bill']['votes'],
        'texts': response['bill']['texts']
    }

    # store in es
    es.index(index=constants.BILL_META_INDEX, id=response['bill']['bill_id'], body=body, request_timeout=30)


def _decode_bill_doc_content(base64_encoded_text, mime_id):
    decoded_text = base64.b64decode(base64_encoded_text, validate=True)
    
    bill_text=None
    if mime_id == 1:
        bill_text = html_decoder(decoded_text)
    elif mime_id == 2:
        try:
            bill_text = pdf_decoder(decoded_text)
        except Exception as e:
            logging.warning('Error encounterd {}'.format(e))
            bill_text = ''
    else:
        logging.warning('A decoder is not defined for mime_id {}'.format(mime_id))
        logging.warning('Returning the encoded text')
        bill_text = base64_encoded_text

    return bill_text


def _get_text(doc_id):
    """
    Retrieves a bill doc (a text version of a bill) from Legiscan through an API call
    :param doc_id (int): ID of the document to be fetched
    :return:
    """
    api_url = "http://api.legiscan.com/?key={}&op=getBillText&id={}".format(API_KEY, doc_id)
    r = urlopen(api_url).read().decode()
    response = json.loads(r)

    if response.get('text', None) is not None:
        # there are bills with date 0000-00-00
        bill_date = response['text']['date']
        if bill_date == '0000-00-00':
            bill_date = '1970-01-01'

        # decoding the doc
        mime_id = response['text']['mime_id']
        encoded_doc = response['text']['doc']
        decoded_doc = _decode_bill_doc_content(encoded_doc, mime_id)

        body = {
            'doc_id': response['text']['doc_id'],
            'date': bill_date,
            'type': response['text']['type'],
            'type_id': response['text']['type_id'],
            'mime': response['text']['mime'],
            'mime_id': response['text']['mime_id'],
            'text_size': response['text']['text_size'],
            'bill_id': response['text']['bill_id'],
            'doc': decoded_doc,
            'encoded_doc': encoded_doc
        }

        # store in es
        es = get_elasticsearch_conn("../../conf/local/credentials.yaml")
        id = "{}_{}".format(response['text']['bill_id'], response['text']['doc_id'])
        es.index(index=constants.BILL_TEXT_INDEX, id=id, body=body, request_timeout=30)


def _update_session_people_es(response, session_id):
    """
    Updates the session_people index on elasticsearch with a new person
    """

    body = {
            'session_id': session_id,
            'people_id': response.get('people_id'),
            'person_hash': response.get('person_hash'),
            'state_id': response.get('state_id'),
            'party_id': response.get('party_id'),
            'party': response.get('party'),
            'role_id': response.get('role_id'),
            'role': response.get('role'),
            'name': response.get('name'),
            'first_name': response.get('first_name'),
            'middle_name': response.get('middle_name'),
            'last_name': response.get('last_name'),
            'suffix': response.get('suffix'),
            'nickname': response.get('nickname'),
            'district': response.get('district'),
            'ftm_eid': response.get('ftm_eid'),
            'votesmart_id': response.get('votesmart_id'),
            'opensecrets_id': response.get('opensecrets_id'),
            'ballotpedia': response.get('ballotpedia'),
            'committee_sponsor': response.get('committee_sponsor'),
            'committee_id': response.get('committee_id')
        }

    # store in es
    es = get_elasticsearch_conn("../../conf/local/credentials.yaml")
    id = '{}_{}'.format(session_id, response['person']['people_id'])
    es.index(index=constants.SESSION_PEOPLE_INDEX, id=id, body=body, request_timeout=30)


def _check_bill_hashes(bill_hashes):
    """
    Verify which hashes have changed
    :param bill_hashes: List of bill hashes
    :return:
    """
    db_conn = get_db_conn("../../conf/local/credentials.yaml")
    cursor = db_conn.cursor()

    q = """
        select 
	        distinct on (bill_id) bill_id, session_id, bill_hash, update_date
        from legiscan_update_metadata.bill_hashes 
        where session_id='{}'
        order by bill_id, update_date DESC
    """.format(
        str(bill_hashes[0]['session_id'])
    )

    try:
        cursor.execute(q)
        our_bill_hashes = cursor.fetchall()
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    our_bill_hashes_df = pd.DataFrame(our_bill_hashes, columns=['bill_id', 'session_id', 'bill_hash', 'update_date'])
    updated_bill_hashes_df = pd.DataFrame(bill_hashes, columns=['bill_id', 'session_id', 'state_id', 'bill_hash', 'file_name'])

    # check for updates
    df = updated_bill_hashes_df.merge(our_bill_hashes_df, how="left", on=['bill_id'],
                                      suffixes=["_updated", "_ours"])

    updated_bills = df[df.bill_hash_updated != df.bill_hash_ours]

    return updated_bills.to_dict("records")


def _get_bill_hashes(bills_session):
    """
    Retrieves the bill hashes of a specific session
    :param bills_session: Bills in a session list
    :return:
    """
    bills_hashes = []
    for file_name, element in bills_session.items():
        bill_json = json.loads(element)
        bills_hashes.append(
            {
                'bill_id': str(bill_json['bill']['bill_id']),
                'session_id': str(bill_json['bill']['session_id']),
                'state_id': str(bill_json['bill']['state_id']),
                'bill_hash': bill_json['bill']['change_hash'],
                'file_name': file_name
            }
        )

    return bills_hashes


def _get_bills_from_dataset(zip_file_content):
    """
    Reads the zip file and filter the bills from the dataset
    :param zip_file_content (base64 encoded zip): The zipped dataset
    :return:
    """
    decoded_text = base64.b64decode(zip_file_content['dataset']['zip'])
    fp = tempfile.TemporaryFile()
    fp.write(decoded_text)
    zip_ref = ZipFile(fp, 'r')

    # bills_session = []
    bills_session = {}

    for file_name in zip_ref.namelist():
        temp = file_name.split('/')
        fkey = temp[-2]
        if fkey == 'bill':
            bills_session[file_name] = zip_ref.read(file_name).decode('utf-8')
            # bills_session.append(zip_ref.read(file_name).decode('utf-8'))

    return bills_session


def _get_people_from_dataset(zip_file_content):
    """
    Reads the zip file and filter people files from the dataset
    args:
        zip_file_content (base64 encoded zip): The zipped dataset 
    """

    decoded_text = base64.b64decode(zip_file_content['dataset']['zip'])
    fp = tempfile.TemporaryFile()
    fp.write(decoded_text)
    zip_ref = ZipFile(fp, 'r')

    people_session = {}
    for file_name in zip_ref.namelist():
        temp = file_name.split('/')
        fkey = temp[-2]
        if fkey == 'people':
            people_session[file_name] = zip_ref.read(file_name).decode('utf-8')

    return people_session


def _get_session_id_from_dataset(zip_file_content):
    """
    Reads the zip file and retrieves the Legiscan given session_id for the session being processed
    args:
        zip_file_content (base64 encoded zip): The zipped dataset 
    """
    decoded_text = base64.b64decode(zip_file_content['dataset']['zip'])
    fp = tempfile.TemporaryFile()
    fp.write(decoded_text)
    zip_ref = ZipFile(fp, 'r')

    # This assumes that all bills have a session_id in their json and the session_id is the same
    # We take the session_id of the first bill we encounter while traversing the zip content
    session_id = None
    
    for file_name in zip_ref.namelist():
        temp = file_name.split('/')
        fkey = temp[-2]
        if fkey=='bill':
            content = zip_ref.read(file_name).decode('utf-8')
            content = json.loads(content)
            session_id = content['bill']['session']['session_id']
            break
    
    return session_id


def get_datasetlist_from_api():
    """
    Get the datasetlist for the weekly updates from Legiscan
    :return: List of all active sessions
    """

    api_url = "https://api.legiscan.com/?key={}&op=getDatasetList".format(API_KEY)

    s3_creds = get_s3_credentials("../../conf/local/credentials.yaml")

    session = boto3.Session(
        aws_access_key_id = s3_creds['aws_access_key_id'],
        aws_secret_access_key = s3_creds['aws_secret_access_key']
    )
    s3 = session.resource('s3')

    key = constants.S3_BUCKET_LEGISCAN_UPDATES + '/datasetlist/' + str(TODAY) + '/datasetlist_' + str(TODAY) + ".pkl"

    # check if we need to call API or retrieve data from S3
    api_calls = 0
    try:
        obj = s3.Object(constants.S3_BUCKET, key).get()['Body'].read()
        response = pickle.loads(obj)
        logging.info('File in S3. No need for an API call')
    except Exception as error:
        # key doesn't exist, then call API
        logging.warning(error)
        logging.info('The dataset does not exist. Calling the API')
        
        # API_CALL_COUNTER = API_CALL_COUNTER + 1
        # logging.info('API calls so far (in this run): {}'.format(API_CALL_COUNTER))
        
        r = urlopen(api_url).read().decode()
        response = json.loads(r)
        api_calls += 1

        file_name = "datasetlist_" + str(TODAY) + ".pkl"

        # store sessions in s3 bucket
        pickle_data = pickle.dumps(response)

        key = constants.S3_BUCKET_LEGISCAN_UPDATES + '/datasetlist/' + str(TODAY) + '/' + file_name
        s3.Object(constants.S3_BUCKET, key).put(Body=pickle_data)

    status = response['status']

    current_sessions = []
    if status == 'OK':
        for element in response['datasetlist']:
            if (element['year_end'] == CURRENT_YEAR) or (element['year_start'] == CURRENT_YEAR):
                current_sessions.append({'state_id': str(element['state_id']),
                                        'session_id': str(element['session_id']),
                                        'session_hash': element['dataset_hash'],
                                        'year_start': element['year_start'],
                                        'year_end': element['year_end'],
                                        'access_key': element['access_key']})
    else:
        logging.info("Error while retrieving the datasetlist from Legiscan", status)
        logging.error("Error while retrieving the datasetlist from Legiscan", status)

    logging.info('Fetched info about {} datasets, and {} of them are sessions for {}'.format(
        len(response['datasetlist']), len(current_sessions), CURRENT_YEAR
    ))

    states_with_current_sessions = [x['state_id'] for x in current_sessions]
    logging.info('States with datasets in {}: {}'.format(CURRENT_YEAR, states_with_current_sessions))

    return current_sessions, api_calls


def get_dataset_from_api(session_id, access_key):
    """
    Fetches the dataset from legiscan through an API call and saves it on S3
    :param session_id: Session id to look for on Legiscan API
    :param access_key: Access key associated to the session id
    :return:
    """

    api_url = "https://api.legiscan.com/?key={}&op=getDataset&id={}&access_key={}"\
        .format(API_KEY, session_id, access_key)

    # check if we already have the pkl for the session data
    s3_creds = get_s3_credentials("../../conf/local/credentials.yaml")

    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )
    s3 = session.resource('s3')

    file_name = "session_info_" + str(session_id) + "_" + str(TODAY) + ".pkl"

    key = constants.S3_BUCKET_LEGISCAN_UPDATES + '/dataset/' + str(TODAY) + '/' + file_name

    api_calls = 0
    try:
        obj = s3.Object(constants.S3_BUCKET, key).get()['Body'].read()
    except Exception as error:
        # no pkl for that key
        logging.warning(error)
        logging.warning('The dataset for the session is not on the S3 bucket. Fetchiing it through an API call')
        r = urlopen(api_url).read().decode()
        api_calls += 1

        response = json.loads(r)

        # store in s3 bucket
        pickle_data = pickle.dumps(response)
        s3.Object(constants.S3_BUCKET, key).put(Body=pickle_data)

    return api_calls


def review_dataset(current_session):
    """
    Review the pkl files obtained from legiscan and organize them by type
    args:
        current_session (str): The file name of the pickle file
    """
    s3_creds = get_s3_credentials("../../conf/local/credentials.yaml")

    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )

    file_name = current_session
    s3_resource = session.resource('s3')

    obj = s3_resource.Object(constants.S3_BUCKET, file_name).get()['Body'].read()
    response = pickle.loads(obj)
    status = response['status']

    if status == 'OK':
        bills_session = _get_bills_from_dataset(response)
        bill_hashes = _get_bill_hashes(bills_session)

        people_session = _get_people_from_dataset(response)
        session_id = _get_session_id_from_dataset(response)
    else:
        # logging.info("Error while retrieving the datasetlist from Legiscan", status)
        logging.error("Error while retrieving the datasetlist from Legiscan", status)
        logging.warning('Was not able to parse the dataset {}'.format(current_session))

    return bill_hashes, bills_session, people_session, session_id


def check_bill_updates(current_session):
    """
    Check which bills for each session had changed
    :param current_session: Sessions that change on last week
    :return:
    """
    #get_dataset_from_api(current_session['session_id'], current_session['access_key'])
    bill_hashes, bills_session, _ , _= review_dataset(current_session)
    bills_updated = _check_bill_hashes(bill_hashes)

    return bills_updated, bills_session


def check_people_updates(current_session):
    """
    Check for new people in the session
    args:
        current_session (str): The file name of the session being processed
    """
    _, _ , people_session, session_id = review_dataset(current_session)

    db_conn = get_db_conn("../../conf/local/credentials.yaml")
    cursor = db_conn.cursor()

    q = """
    select 
        people_id,
        person_hash
    from 
        clean.session_people
        where session_id={} 
    """.format(session_id)

    try:
        cursor.execute(q)
        people_in_db = cursor.fetchall()
        people_in_db = pd.DataFrame(people_in_db, columns=['people_id', 'person_hash'])
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        people_in_db = pd.DataFrame(columns=['people_id', 'person_hash'])

    
    logging.debug('number of people in the db for the session: {}'.format(people_in_db.shape[0]))
    logging.debug(people_in_db.head(2))

    people_in_legiscan = pd.DataFrame()
    for fn, content in people_session.items():
        content = json.loads(content)
        d = dict()
        d['file_name'] = fn
        d['people_id'] = content['person']['people_id']
        d['person_hash'] = content['person']['person_hash']

        people_in_legiscan = people_in_legiscan.append(d, ignore_index=True)
    
    logging.debug('number of people in the dataset for the session: {}'.format(people_in_legiscan.shape[0]))
    logging.debug(people_in_legiscan.head(2))

    if len(people_in_db) > 0:
        df = people_in_legiscan.merge(people_in_db, how="left", on=['people_id'],
                                      suffixes=["_updated", "_ours"])

        # Rows that exist in the fetched dataset but not in the db
        msk = df['person_hash_ours'].isnull()
        updated_people = df[msk]
    else:
        updated_people = people_in_legiscan
        
    return updated_people.to_dict("records"), people_session, session_id 


def check_session_updates(current_sessions):
    """
    Checks which sessions from Legiscan has been updated from the last week
    :param current_sessions: Sessions that are in session this year
    :return:
    """
    db_conn = get_db_conn('../../conf/local/credentials.yaml')
    cursor = db_conn.cursor()

    # get sessions that we have
    q = "select session_id, state_id, session_hash from legiscan_update_metadata.session_hashes;"

    try:
        logging.info('writing dataset entry to the database')
        # cursor.execute(q, current_sessions)
        cursor.execute(q)
        our_hashes = cursor.fetchall()
        # db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    update_hashes_df = pd.DataFrame(current_sessions, columns=['state_id', 'session_id', 'session_hash', 'year_end', 'year_start',
                                                              'access_key'])
    our_hashes_df = pd.DataFrame(our_hashes, columns=['session_id', 'state_id', 'session_hash'])

    hashes_df = update_hashes_df.merge(our_hashes_df, how="left", on=['session_id', 'state_id'],
                                       suffixes=['_update', '_ours'])

    # identify which sessions have been updated
    updates = hashes_df[hashes_df.session_hash_update != hashes_df.session_hash_ours]

    logging.info('For each current dataset, we compare the hashes that we stored in the DB')
    logging.info('Found {} sessions with changed hashes'.format(len(updates)))
    logging.info('States with updated sessions: {}'.format(
        updates['state_id'].tolist()
    ))

    return updates.to_dict('records')


def check_bill_text_updates(bill_id, bill_texts):
    """
    Check for new bill text versions 
    args:
        bill_id (int): ID of the bill
        bill_texts (List[Dict]): The 'texts' field from the bill json returned from Legican getBill
    """

    db_conn = get_db_conn("../../conf/local/credentials.yaml")
    cursor = db_conn.cursor()

    q = """
        select 
            doc_id
        from clean.bill_docs
        where bill_id = {}
    """.format(bill_id)

    try:
        cursor.execute(q)
        docs_in_db = cursor.fetchall()
        docs_in_db = set([x[0] for x in docs_in_db])
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        docs_in_db = set([])

    docs_in_legiscan = set([x['doc_id'] for x in bill_texts])

    # Bills that exist in Legiscan but not in our DB
    diff = docs_in_legiscan.difference(docs_in_db)

    return diff  


def update_bills_on_es(bills_updated, bill_contents):
    """
    Store the new docs updated on ES
    :param bills_updated: List of bill ids that have changed
    :return:
    """

    api_calls = 0
    for element in bills_updated:
        fname = element['file_name']
        contents = bill_contents[fname]
        response = json.loads(contents)

        logging.info('Updating metadata of bill ID {}'.format(response['bill']['bill_id']))
        _update_bill_metadata(response=response)

        # Checking whether there are new bill versions
        logging.info('Metadata updated on elasticsearch. Now on to bill texts')
        bill_id = response['bill']['bill_id']
        bill_texts = response['bill']['texts']
        new_bill_docs = check_bill_text_updates(bill_id, bill_texts)
        # logging.info('There are {} new bill docs. But skipping for now to save API calls'.format(len(new_bill_docs)))
       
        # each new doc needs an api call
        api_calls += len(new_bill_docs)
        
        logging.info('new bill_docs: {}'.format(new_bill_docs))

        if len(new_bill_docs) > 0:
            for doc_id in new_bill_docs:
                _get_text(doc_id)

            logging.info('Fetched {} new bill docs and stored in Elasticsearch'.format(len(new_bill_docs)))
        else:
            logging.info('No new bill docs since last update')

    return api_calls


def update_people_on_es(people_updated, people_contents, session_id):
    """
    Store the new people updated on ES
    args:
        people_updated (List[Dict]): 
    :return:
    """

    for element in people_updated:
        fname = element['file_name']
        contents = people_contents[fname]
        response = json.loads(contents)

        logging.info('Updating person with ID {}'.format(response['person']['people_id']))
        _update_session_people_es(response, session_id)

    logging.info('Updated {} people on elasticsearch'.format(len(people_updated)))


def _update_bill_amendments(db_conn, bill_ids):
    """
    Update bill amendments on DB
    :param db_conn: Connection to DB
    :param bill_ids: List of bill ids to update
    :return:
    """

    logging.info("Updating bill_amendments table")
    
    q = """
        insert into clean.bill_amendments
        with step_1 as(
        select 
            bill_id, 
            jsonb_array_elements(amendments::jsonb) as amendments
        from raw.bill_meta_es
        )
        select 
            cast(bill_id as integer) as bill_id, 
            cast(amendments->>'amendment_id' as integer) as amendment_id, 
            case 
                when amendments->>'date' = '0000-00-00' then '1970-01-01' 
                else cast(amendments->>'date' as date)
            end amendment_date,
            amendments->>'chamber', 
            cast(amendments->>'adopted' as smallint) as adopted, 
            amendments->>'title' as amendment_title, 
            amendments->>'description' as amendment_description, 
            amendments->>'url', 
            amendments->>'state_link'
        from 
            step_1
        where 
            bill_id in {}
    """.format(tuple(bill_ids))

    try:
        cursor = db_conn.cursor()

        try:
            logging.info('updating bill_amendments table from ES')
            cursor.execute(q)
        except (Exception, psycopg2.IntegrityError) as error:
            logging.error(error)
            db_conn.rollback()
        else:
            db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _update_bill_committees(db_conn, bill_ids):
    """
    Update bill committess table on DB
    :param db_conn: Connection to DB
    :param bill_ids: List of bill ids to update
    :return:
    """
    logging.info("updating bill_committees table...")
    q = """
    insert into clean.bill_committees
    select 
        cast(bill_id as integer), 
        cast(committee->'committee_id' as integer) as committee_id, 
        committee->'chamber' as chamber, 
        committee->'name' as name
    from 
        raw.bill_meta_es
    where 
        bill_id in {}
    """.format(tuple(bill_ids))

    try:
        cursor = db_conn.cursor()

        try:
            logging.info('updating bill_committees table from ES')
            cursor.execute(q)
        except (Exception, psycopg2.IntegrityError) as error:
            logging.error(error)
            db_conn.rollback()
        else:
            db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _update_bill_docs(db_conn, bill_ids):
    """
    Update bill docs on DB
    :param db_conn: Connection to DB
    :param bill_ids: List of bill ids to update
    :return:
    """
    logging.info("updating bill_docs table")
    q = """
    insert into clean.bill_docs
    select 
        cast(doc_id as integer), 
        cast(bill_id as integer), 
        type as doc_type, 
        doc_date, 
        cast(text_size as integer)
    from 
        raw.bill_text_es
    where 
        bill_id in {}
    """.format(tuple(bill_ids))

    try:
        cursor = db_conn.cursor()

        try:
            logging.info('updating bill_docs table from ES')
            cursor.execute(q)
        except (Exception, psycopg2.IntegrityError) as error:
            logging.error(error)
            db_conn.rollback()
        else:
            db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _update_bill_events(db_conn, bill_ids):
    """
    Update bill events table in DB
    :param db_conn: Connection to DB
    :param bill_ids: List of bill ids to update
    :return:
    """
    logging.info("updating bill_events table...")
    q = """
    insert into clean.bill_events
    with step_1 as(
        select 
            bill_id,
            jsonb_array_elements(history::jsonb) as events
        from 
            raw.bill_meta_es
    )
    select 
        cast(bill_id as integer) as bill_id, 
        case 
            when events->>'date' = '0000-00-00' then '1970-01-01'
            else cast(events->>'date' as date) 
        end event_date,
        events->>'action' as action, 
        events->>'chamber' as chamber, 
        cast(events->>'importance' as smallint) as important,
        md5(row(events->>'action', events->>'chamber', events->>'date')::text) as event_hash
    from 
        step_1
    where 
        bill_id in {}
    """.format(tuple(bill_ids))

    try:
        cursor = db_conn.cursor()

        try:
            logging.info('updating bill_events table from ES')
            cursor.execute(q)
        except (Exception, psycopg2.IntegrityError) as error:
            logging.error(error)
            db_conn.rollback()
        else:
            db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _update_bill_progress(db_conn, bill_ids):
    """
    Update bill progress table on DB
    :param db_conn: Connection to DB
    :param bill_ids: List of bill ids to update
    :return:
    """
    logging.info("updating bill_progress table...")
    q = """
    insert into clean.bill_progress
    with step_1 as(
        select 
            bill_id,
            jsonb_array_elements(progress::jsonb) as progress
        from 
            raw.bill_meta_es
    )

    select 
        cast(bill_id as integer) as bill_id, 
        case
            when progress->>'date' = '0000-00-00' then '1970-01-01'
            else cast(progress->>'date' as date) 
        end progress_date, 
        cast(progress->>'event' as smallint) as event
    from 
        step_1
    where 
        bill_id in {}
    """.format(tuple(bill_ids))

    try:
        cursor = db_conn.cursor()

        try:
            logging.info('updating bill_progress table from ES')
            cursor.execute(q)
        except  (Exception, psycopg2.IntegrityError) as error:
            logging.error(error)
            db_conn.rollback()
        else:
            db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _update_bill_sponsors(db_conn, bill_ids):
    """
    Update bill sponsors table on DB
    :param db_conn: Connection to DB
    :param bill_ids: List of bill ids to update
    :return:
    """
    logging.info("updating bill_sponsors table...")
    q = """
    insert into clean.bill_sponsors
    with step_1 as(
        select 
            bill_id,
            jsonb_array_elements(sponsors::jsonb) as sponsors
        from 
            raw.bill_meta_es
    )
    select 
        cast(sponsors->>'people_id' as integer) as sponsor_id, 
        cast(bill_id as integer), 
        cast(sponsors->>'party_id' as smallint) as party_id, 
        sponsors->>'role',
        cast(sponsors->>'sponsor_type_id' as smallint) as sponsor_type
    from 
        step_1
    where 
        bill_id in {}
    """.format(tuple(bill_ids))

    try:
        cursor = db_conn.cursor()

        try:
            logging.info('updating bill_sponsors table from ES')
            cursor.execute(q)
        except (Exception, psycopg2.IntegrityError) as error:
            logging.error(error)
            db_conn.rollback()
        else:
            db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _update_bill_votes(db_conn, bill_ids):
    """
    Update bill votes table in DB
    :param db_conn: Connection to DB
    :param bill_ids: List of bill ids to update
    :return:
    """
    logging.info("updating bill_votes table...")
    q = """
    insert into clean.bill_votes  
    with step_1 as(
        select 
            bill_id, 
            url, 
            state_link,
            jsonb_array_elements(votes::jsonb) as votes
        from 
            raw.bill_meta_es
    )
    
    select 
        cast(votes->>'roll_call_id' as integer) as vote_id, 
        cast(bill_id as integer), 
        case 
            when votes->>'date' = '0000-00-00' then '1970-01-01'
            else cast(votes->>'date' as date) 
        end vote_date, 
        votes->>'desc' as votes_description, 
        cast(votes->>'yea' as smallint),
        cast(votes->>'nay' as smallint), 
        cast(votes->>'nv' as smallint), 
        cast(votes->>'absent' as smallint), 
        cast(votes->>'total' as smallint), 
        cast(votes->>'passed' as boolean), 
        votes->>'chamber', 
        url, 
        state_link
    from 
        step_1
    where 
        bill_id in {}
    """.format(tuple(bill_ids))

    try:
        cursor = db_conn.cursor()

        try:
            logging.info('updating bill_votes table from ES')
            cursor.execute(q)
        except (Exception, psycopg2.IntegrityError) as error:
            logging.error(error)
            db_conn.rollback()
        else:
            db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _update_bills(db_conn, bill_ids):
    """
    Update bills on DB
    :param db_conn: Connection to DB
    :param bill_ids: List of bill_ids
    :return:
    """
    logging.info("updating bills table...")
    q = """
    insert into clean.bills
    with step_1 as(
        select 
            bill_id, 
            session->'session_id' as session_id, 
            bill_type, 
            title,
            description,
            bill_number,
            jsonb_array_elements(subjects::jsonb) as subjects,
            jsonb_array_elements(progress::jsonb) as progress,
            body as introduced_body,
            url,
            state_link
        from 
            raw.bill_meta_es
    )

    select 
        cast(bill_id as integer), 
        cast(session_id as integer), 
        bill_type, 
        bill_number,
        state,
        title,
        description,
        subjects->>'subject_name' as subjects,
        case 
            when progress->>'date' = '0000-00-00' then '1970-01-01'
            else cast(progress->>'date' as date) 
        end introduced_date, 
        introduced_body, 
        url,
        state_link
    from 
        step_1
    where 
        progress->>'event' = '1'
        and bill_id in {}
    """.format(tuple(bill_ids))

    try:
        cursor = db_conn.cursor()

        try:
            logging.info('updating bills table from ES')
            cursor.execute(q)
        except (Exception, psycopg2.IntegrityError) as error:
            logging.error(error)
            db_conn.rollback()
        else:
            db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _update_session_people_db(db_conn, people_ids):
    """
    Update session people table on DB
    :param db_conn: Connection to DB
    :param people_ids: List of people_ids to update
    :return:
    """
    logging.info("updating session_people table...")
    q = """
    insert into clean.session_people
    with step_1 as(
        select 
            session_id, 
            people_id,
            person_hash,
            state_id,
            party_id,
            party,
            role_id,
            role,
            name, 
            first_name,
            middle_name,
            last_name,
            suffix,
            nickname,
            district,
            ftm_eid,
            votesmart_id,
            opensecrets_id,
            ballotpedia,
            committee_sponsor,
            committee_id
        from 
            raw.session_people_es
    )
    select 
        cast(session_id as integer),   
        cast(people_id as integer) as people_id, 
        person_hash, 
        state_id, 
        cast(party_id as smallint) as party_id, 
        party, 
        cast(role_id as integer) as role_id, 
        role, 
        name,
        first_name, 
        last_name, 
        middle_name, 
        suffix,
        nickname, 
        district, 
        ftm_eid as ftm_id, 
        votesmart_id,
        opensecrets_id, 
        ballotpedia, 
        cast(committee_sponsor as boolean) as committee_sponsor,
        cast(committee_id as smallint) as committee_id
    from 
        step_1
    where 
        bill_id in {}
    """.format(tuple(people_ids))
    # TODO -- This will break if there's only one element in the list Fix!

    try:
        cursor = db_conn.cursor()

        try:
            logging.info('updating session_people table from ES')
            cursor.execute(q)
        except (Exception, psycopg2.IntegrityError) as error:
            logging.error(error)
            db_conn.rollback()
        else:
            db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def _update_sessions(db_conn, bill_ids):
    """
    Update sessions table on DB
    :param db_conn: Connection to DB
    :param bill_ids: List of bill ids to update
    :return:
    """
    logging.info("updating sessions table...")
    q = """
    insert into clean.sessions
    with step_1 as(
        select 
            cast(session->'session_id' as integer) as session_id, 
            session->'session_title' as session_title, 
            cast(session->'year_start' as smallint) as year_start, 
            cast(session->'year_end' as smallint) as year_end, 
            cast(state_id as smallint) as state_id, 
            cast(cast(session->'special' as integer) as boolean) as special
        from 
            raw.bill_meta_es
        where 
            bill_id in {}
    ), 
    
    step_2 as(
        select 
            session_id, 
            session_title, 
            year_start, 
            year_end, 
            state_id, 
            special
        from 
            step_1
        group by 
            session_id, 
            session_title, 
            year_start, 
            year_end, 
            state_id, 
            special
    )
    
    select 
        session_id, 
        session_title, 
        year_start, 
        year_end, 
        state_id, 
        special
    from 
        step_2
    """.format(tuple(bill_ids))

    try:
        cursor = db_conn.cursor()

        try:
            logging.info('updating sessions table from ES')
            cursor.execute(q)
        except (Exception, psycopg2.IntegrityError) as error:
            logging.error(error)
            db_conn.rollback()
        else:
            db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def update_db_from_es(bills_updated=None, people_updated=None):
    """
    Update relational DB from elastic search tables
    :param bills_updated: List of bills that have been updated in the last week
    :return:
    """
    db_conn = get_db_conn('../../conf/local/credentials.yaml')
    if bills_updated is not None:
        bill_ids = [element['bill_id'] for element in bills_updated]
        # update postgres tables
        _update_bill_amendments(db_conn, bill_ids)
        _update_bill_committees(db_conn, bill_ids)
        _update_bill_docs(db_conn, bill_ids)
        _update_bill_events(db_conn, bill_ids)
        _update_bill_progress(db_conn, bill_ids)
        _update_bill_sponsors(db_conn, bill_ids)
        _update_bill_votes(db_conn, bill_ids)
        _update_bills(db_conn, bill_ids)
        _update_sessions(db_conn, bill_ids)
    
    if people_updated is not None:
        pepole_ids = [element['people_id'] for element in people_updated]
        _update_session_people_db(db_conn, pepole_ids)
    

def update_session_hash_in_db(session_updated):
    """
    Update the hashes of sessions that had changed
    :param session_updated:
    :return:
    """
    db_conn = get_db_conn('../../conf/local/credentials.yaml')
    cursor = db_conn.cursor()

    # check if the session already exists

    q = """
        select 
            session_id,
            state_id
        from
            legiscan_update_metadata.session_hashes
        where session_id = '{}' 
        and state_id = '{}'
    """.format(session_updated['session_id'], session_updated['state_id'])

    try:
        logging.info("session_hashes select")
        cursor.execute(q)
        existing_session = cursor.fetchall()
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
            logging.error(error)

    # insert or update
    if len(existing_session) > 0:
        # udpate existing session
        q = """
             update legiscan_update_metadata.session_hashes set session_hash = '{}', update_date = '{}' where session_id = '{}' 
             and state_id = '{}' 
        """.format(session_updated['session_hash_update'], TODAY, session_updated['session_id'],
                   session_updated['state_id'])
    else:
        q = """
            insert into legiscan_update_metadata.session_hashes (session_id, state_id, session_hash, update_date) 
            values ('{}', '{}', '{}', '{}')
        """.format(session_updated['session_id'], session_updated['state_id'], session_updated['session_hash_update'],
                   TODAY)

    try:
        logging.info('updating session_hashes table')
        cursor.execute(q)
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)


def update_bill_hashes_in_db(bills_updated):
    """
    Update the hashes of bills changed on our db
    :param bills_updated:
    :return:
    """
    # flatten list of lists
    # bills_to_update = list(itertools.chain(*bills_updated))

    db_conn = get_db_conn('../../conf/local/credentials.yaml')
    cursor = db_conn.cursor()

    # check if we need to insert or update
    q = """
        select 
            bill_id,
            session_id
        from 
            legiscan_update_metadata.bill_hashes
    """

    try:
        logging.info("session_hashes select")
        cursor.execute(q)
        existing_bills = cursor.fetchall()
        db_conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)

    # check which sessions need to be inserted vs updated
    existing_bills_df = pd.DataFrame(existing_bills, columns=["bill_id", "session_id"])
    bills_updated_df = pd.DataFrame(bills_updated)

    # bills that require update
    bills_to_update = bills_updated_df.merge(existing_bills_df, how="inner")
    if bills_to_update.shape[0] > 0:
        for element in bills_updated:
            q = """
            update legiscan_update_metadata.bill_hashes set bill_hash = '{}', update_date = '{}' where bill_id = '{}' 
            and session_id = '{}' """.format(element['bill_hash_updated'], TODAY,
                                             element['bill_id'], element['session_id_ours'])

            try:
                logging.info('updating bill_hashes table from ES')
                cursor.execute(q)
                db_conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(error)

    # sessions that need to be inserted
    bills_to_insert = bills_updated_df.merge(existing_bills_df, how="left")
    if bills_to_insert.shape[0] > 0:
        for element in bills_to_insert.to_dict('records'):
            q = """
                insert into legiscan_update_metadata.bill_hashes (bill_id, session_id, bill_hash, update_date) 
                values ('{}', '{}', '{}', '{}')
            """.format(element['bill_id'], element['session_id_updated'], element['bill_hash_updated'], TODAY)

            try:
                logging.info('inserting new bill hashes in bill_hashes table')
                cursor.execute(q)
                db_conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                logging.error(error)


def retrieve_session_file_name_pkl(session_updated):
    """
    Retrieves
    :param i: Index of the session to retrieve
    :return:
    """
    s3_creds = get_s3_credentials("../../conf/local/credentials.yaml")

    session = boto3.Session(
        aws_access_key_id=s3_creds['aws_access_key_id'],
        aws_secret_access_key=s3_creds['aws_secret_access_key']
    )
    s3 = session.client('s3')

    key = constants.S3_BUCKET_LEGISCAN_UPDATES + '/dataset/' + str(TODAY)
    objects = s3.list_objects_v2(Bucket=constants.S3_BUCKET, Prefix=key)['Contents']

    # get the pkl files for each state
    session_id = session_updated['session_id']
    for element in objects:
        if session_id in element['Key']:
            file_name = element['Key']
            break

    return file_name


def update_data_from_legiscan():
    """
    Main function that coordinates the checkup and update of bills into ES and DB
    :return:
    """
    number_of_api_calls = 0

    logging.info("Fetching the datasetlist")
    current_sessions, api_calls = get_datasetlist_from_api()
    number_of_api_calls += api_calls

    logging.info("Identifying sessions that were updated")
    sessions_updated = check_session_updates(current_sessions)
    # if there were sessions updated last week, check which bills changed

    # first get all the session_info from legiscan and stored them on S3, then extract and update all data related
    for i in range(len(sessions_updated)):
        current_session = sessions_updated[i]
        api_calls = get_dataset_from_api(current_session['session_id'], current_session['access_key'])
        number_of_api_calls += api_calls

    logging.info('Session datasets fetched from legiscan. API calls used so far : {}'.format(number_of_api_calls))
    
    # get data from pkls
    for session_updated in sessions_updated:
        session_file_name = retrieve_session_file_name_pkl(session_updated)
        logging.info("updating bills from session {}".format(session_updated))
        
        bills_updated, bill_contents = check_bill_updates(session_file_name)
        people_updated, people_contents, session_id = check_people_updates(session_file_name)
        
        logging.info("updating session {}".format(session_id))
        logging.info("{} bills to update ".format(len(bills_updated)))
        logging.info("{} people to update ".format(len(people_updated)))
        
        updated_data = False
        if len(bills_updated) > 0:
            logging.info("inserting updated bills in es")
            api_calls = update_bills_on_es(bills_updated, bill_contents)
            number_of_api_calls += api_calls
            logging.debug('api calls so far: {}'.format(number_of_api_calls))

            updated_data = True

            # # TODO -- remove
            # update_db_from_es(bills_updated)

            # logging.info("Storing & updating new bill hashes in the DB")
            update_bill_hashes_in_db(bills_updated)

            # logging.info("Storing & updating new session hashes in the DB")
            update_session_hash_in_db(session_updated)
            # logging.info("There are no sessions updated")
        else:
            update_session_hash_in_db(session_updated)
            logging.info("There were no updated/new bills")

        if len(people_updated) > 0: 
            update_people_on_es(people_updated, people_contents, session_id)
            updated_data = True

        if updated_data:
            # logging.info("Updating the DB from ES")
            # update_db_from_es(bills_updated)
            pass

        logging.info("session {} is done. Used up {} API calls".format(session_id, number_of_api_calls))

    logging.info("successfully updated the data. Used {} API calls".format(number_of_api_calls))

        
if __name__ == '__main__':
    update_data_from_legiscan()
