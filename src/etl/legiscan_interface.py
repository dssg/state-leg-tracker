import json
import base64
import logging

from urllib.request import urlopen
from typing import List, Dict

from src.utils.decoders import html_decoder, pdf_decoder


# TODO: write a util function to create the url and get the json given the required api function and patamers 
def get_state_sessions(api_key: str, state: str) -> List[Dict]:
    """ Get a list of state ids for a given state abbreviation"""
    api_url = "https://api.legiscan.com/?key={0}&op=getSessionList&state={1}".format(api_key, state)
    r = urlopen(api_url).read().decode()
    response = json.loads(r)
    status = response['status']
    
    if status=='ERROR':
        logging.error(response.get('alert').get('message')) 
        logging.error(response.get('alert').get('contact')) 

    return response['sessions']


def get_bills_in_session(api_key: str, session_id: int) -> List[Dict]:
    """ Get a list of bills in a session """
    api_url = "https://api.legiscan.com/?key={0}&op=getMasterList&id={1}".format(api_key, session_id)

    r = urlopen(api_url).read().decode()
    response = json.loads(r)

    l = response['masterlist']
    l.pop('session')
    
    return l


def get_bill_info(api_key: str, bill_id: int) -> Dict:
    """ Get information of a bill """
    api_url = "https://api.legiscan.com/?key={0}&op=getBill&id={1}".format(api_key, bill_id)

    r = urlopen(api_url).read().decode()
    response = json.loads(r)

    return response['bill']


def get_bill_content(api_key: str, bill_id: int) -> Dict:
    """ Get the details and text of a bill """

    logging.debug('Fetching contents of bill_id: {}'.format(bill_id))
    
    api_url = "https://api.legiscan.com/?key={0}&op=getBillText&id={1}".format(api_key, bill_id)

    r = urlopen(api_url).read().decode()
    response = json.loads(r)
    bill_details = response['text']

    mime_id = bill_details['mime_id']
    
    encoded_text = bill_details['doc']
    decoded_text = base64.b64decode(encoded_text, validate=True)

    # Mapping the MIME type to the appriprate decoder
    logging.info('Decoding bill content for MIME type {}'.format(mime_id))
    decoder_mapper = {1: html_decoder, 2: pdf_decoder}
    bill_content = decoder_mapper[mime_id](decoded_text)
    logging.debug(bill_content)

    bill_details['doc_decoded'] = bill_content

    return bill_details

def get_available_datasets(api_key: str):
    """ Get datasets that are available to download at the time of running """
    
    api_url = "https://api.legiscan.com/?key={0}&op=getDatasetList".format(api_key)
    r = urlopen(api_url).read().decode()
    response = json.loads(r)
    dataset_list = response.get('datasetlist')
    # logging.debug('num datasets: {}'.format(len(response)))

    # logging.debug(dataset_list)

    return dataset_list


def get_dataset_content(api_key: str, session_id: int, access_key: str):
    """ 
        For a dataset (data for a single session) that is available to download, 
        fetch the zip encoded dataset content
    """
    
    api_url = "https://api.legiscan.com/?key={0}&op=getDataset&id={1}&access_key={2}".format(api_key, session_id, access_key)
    logging.info('Fetching dataset for session {} '.format(session_id))
    r = urlopen(api_url).read().decode()
    response = json.loads(r)

    encoded_content = response.get('dataset').get('zip')
    mime_type = response.get('dataset').get('mime_type')

    return encoded_content, mime_type
    
    