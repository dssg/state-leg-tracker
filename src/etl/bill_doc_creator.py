import json
import logging
import tempfile
import base64
import multiprocessing as mp

from zipfile import ZipFile

from src.utils.decoders import html_decoder, pdf_decoder
from src.utils import project_constants as constants


def parse_session_zip_file(zip_file_content, s3, s3_target):
    """ Parse a zip file of a session
        Returns a list of dictionaries where each entry is a bill.
        The bill contains the bill information, bill text, and the voting records
    """
    fp = tempfile.TemporaryFile()
    fp.write(zip_file_content)
    process_name = mp.current_process().name

    zip_ref = ZipFile(fp, 'r')

    # Dictionaries for holding the bill info, bill text and the voting records
    bills = dict()
    texts = dict()
    roll_calls = dict()

    for fname in zip_ref.namelist():
        content = zip_ref.read(fname)
        content = json.loads(content)
    
        if 'bill' in fname:
            bill_contents = content.get('bill')
            bills[bill_contents.get('bill_id')] = bill_contents
        elif 'text' in fname:
            text_content = content.get('text')
            texts[text_content.get('doc_id')] = text_content

        elif 'vote' in fname:
            roll_call_content = content.get('roll_call')
            roll_calls[roll_call_content.get('roll_call_id')] = roll_call_content
    
    for _, bill_info in bills.items():
        # merging texts 
        d = dict()
        d.update(bill_info)

        # Merging the bill text
        bill_docs = d.pop('texts')
        if bill_docs:
            t = list()
            for doc in bill_docs:
                # Joining the complete bill text with the text information
                text_content = texts[doc.get('doc_id')]
                logging.info('{}: decoding doc_id: {}, of bil_id {}'.format(
                        process_name,
                        text_content.get('doc_id'), 
                        text_content.get('bill_id'),
                    )
                )

                # Replacing the encoded content with the bill text
                encoded_text = text_content.get('doc')
                mime_id = text_content.get('mime_id')
                text_content['doc'] = _decode_bill_doc_content(encoded_text, mime_id)

                doc.update(text_content)
                t.append(doc)

            bill_docs = t

        d['texts'] = bill_docs

        # Merging the bill roll_calls
        votes = d.pop('votes')
        if votes:
            t = list()
            for rc in votes:   
                rc.update(roll_calls[rc.get('roll_call_id')])
                t.append(rc)

            votes = t
        
        d['votes'] = votes

        s3_bucket = constants.S3_BUCKET
        fn = '{}_{}.json'.format(d['session_id'], d['bill_id'])
        fkey = '{}/{}'.format(s3_target, fn)

        s3.Bucket(s3_bucket).put_object(Key=fkey, Body=json.dumps(d))


def _decode_bill_doc_content(base64_eoncoded_text, mime_id):
    decoded_text = base64.b64decode(base64_eoncoded_text, validate=True)
    
    bill_text=''
    if mime_id == 1:
        bill_text = html_decoder(decoded_text)
    elif mime_id == 2:
        bill_text = pdf_decoder(decoded_text)
    else:
        logging.warning('A decoder is not defined for mime_id {}'.format(mime_id))
        logging.warning('Returning empty string')
        
    return bill_text