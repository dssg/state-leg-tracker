import re
import logging
import sys

from pdfreader import PDFDocument, SimplePDFViewer
from bs4 import BeautifulSoup

def _extract_strings_per_page(p, viewer):
    """navigate into specific page p and render its content into a single string"""
    viewer.navigate(p)
    viewer.render()
    strings = viewer.canvas.strings
    page_content = " ".join(strings)

    return page_content


def pdf_decoder(base64_decoded):
    """Decodes a base64 representation of pdf file into string"""
    try:
        pdf_doc = PDFDocument(base64_decoded)
    except: 
        logging.error('encountered error {}'.format(sys.exc_info()[0]))
        logging.warning('Returning empty string for bill content')

        return ''

    all_pages = len([p for p in pdf_doc.pages()])

    viewer = SimplePDFViewer(base64_decoded)
    content = []

    for page in range(1, (all_pages + 1)):
        content.append(_extract_strings_per_page(page, viewer))

    bill_text = " ".join(content)

    return bill_text


def html_decoder(base64_decoded):
    """ Decode a base64 representation of a html type bill into string"""
    bs = BeautifulSoup(base64_decoded)

    for p in bs.find_all('p'):
        if p.string:
            p.string.replace_with(p.string.strip())
        
    # strip white space
    bill_text = re.sub(r'\n\s*\n', r'\n', bs.get_text().strip())

    return bill_text
