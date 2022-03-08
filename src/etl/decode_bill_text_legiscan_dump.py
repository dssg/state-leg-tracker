import os
import sys
import json
import logging
import multiprocessing as mp
import base64

from src.utils.decoders import html_decoder, pdf_decoder
from src.utils import project_constants as constants

logging.basicConfig(level=logging.INFO, filename="../../logs/decode_bills_2021_dump.DEBUG", filemode='w')
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))

# data_dump_path = '/mnt/data/projects/aclu_leg_tracker/legiscan_dump_20200615_processed'

data_dump_path = '/mnt/data/projects/aclu_leg_tracker/legiscan_dump_20210709_processed'

def _get_subdir(dir_path):
    """In a given directory, return all the immediate subdirectories"""

    subdir = [
        x for x in os.listdir(dir_path) 
        if os.path.isdir(os.path.join(dir_path, x))
    ]

    return subdir


def _decode_bill_doc_content(base64_eoncoded_text, mime_id):
    decoded_text = base64.b64decode(base64_eoncoded_text, validate=True)
    
    bill_text=None
    if mime_id == 1:
        bill_text = html_decoder(decoded_text)
    elif mime_id == 2:
        bill_text = pdf_decoder(decoded_text)
    else:
        logging.warning('A decoder is not defined for mime_id {}'.format(mime_id))
        logging.warning('Returning None')
        
    return bill_text


def _subprocess_target(text_files_list, text_path):
    """Decodes a given list of text file"""

    process_name = mp.current_process().name

    logging.info('{}:Decoding {} documents'.format(process_name, len(text_files_list)))

    for txt in text_files_list:
        with open(os.path.join(text_path, txt)) as txt_fp:
            content = json.load(txt_fp)
        
        txt_content = content['text']

        # Check whether the doc_decoded key exists and whether the value is None
        if txt_content.get('doc_decoded') is not None:
            logging.warning('The text content of file {} with bill_id {} and doc_id {} seems to have already been decoded. Skipping!'.format(
                txt, txt_content.get('bill_id'), txt_content.get('doc_id')
            ))
            continue

        encoded_text = txt_content.get('doc')
        mime_id = txt_content.get('mime_id')

        if encoded_text is not None:
            decoded_txt = _decode_bill_doc_content(encoded_text, mime_id)
            txt_content['doc_decoded'] = decoded_txt
            content['text'] = txt_content
            
            with open(os.path.join(text_path, txt), 'w') as txt_fp:
                json.dump(content, txt_fp)


def decode_texts(data_dump_path, n_jobs=-1):
    states = _get_subdir(data_dump_path)

    for state in states:
        tpth = os.path.join(data_dump_path, state)

        # Each session has a sub directory
        sessions = _get_subdir(tpth)

        for session in sessions:
            logging.info('Processing state {}, session {}'.format(state, session))
            tpth2 = os.path.join(tpth, session)

            text_path = os.path.join(tpth2, 'text')
            text_files = os.listdir(text_path) if os.path.isdir(text_path) else []

            num_files = len(text_files)

            # Parallelizing the decoding process
            if (n_jobs > mp.cpu_count()) or n_jobs==-1:
                n_jobs = mp.cpu_count()
            
            # Assigning jobs
            chunk_size, mod = divmod(num_files, n_jobs)
            chunks_list = [chunk_size] * n_jobs

            # Distributing the remainder
            for i in range(mod):
                chunks_list[i] = chunks_list[i] + 1 

            logging.info('Job assignments : {}'.format(chunks_list))

            # Multiprocessing
            # manager = mp.Manager()

            idx_cursor = 0
            jobs = []

            for i, chunk_size in enumerate(chunks_list):
                p = mp.Process(
                    name=f'p{i}',
                    target=_subprocess_target,
                    kwargs={
                        'text_files_list': text_files[idx_cursor:(idx_cursor+chunk_size)],
                        'text_path': text_path,
                    }
                )

                jobs.append(p)
                p.start()

                idx_cursor = idx_cursor + chunk_size 
    
            for proc in jobs:
                proc.join()


        logging.info('Successfully completed decoding bills in state {}'.format(state))



if __name__ == '__main__':
    decode_texts(data_dump_path)

            


