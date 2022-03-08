import os
import re
import logging
import pandas as pd

# This script generates a summary of the dump we received from Legiscan 
# Requies the dump to be unzipped and organized by state
# Unzipping bash script -- unzip_organize_legiscan_dump.sh

data_dump_path = '/mnt/data/projects/aclu_leg_tracker/legiscan_dump_20200615_processed'


def _get_subdir(dir_path):
    """In a given directory, return all the immediate subdirectories"""

    subdir = [
        x for x in os.listdir(dir_path) 
        if os.path.isdir(os.path.join(dir_path, x))
    ]

    return subdir



def summarize_dump(data_dump_path, save_target=None):
    """ Summarizes the data dump acquired from legiscan in June, 2020
        Requires the dump to be unzipped and organized in the following manner:
        state/session/bills/.json
        state/session/people/.json
        state/session/texts/.json
        This function walks through the dump and summarizes the amount of data we have

        Args:
            data_dump_path (str): Where the unzipped dump is located
            save_target (str): If provided, a .csv file will be saved in the location
    """
    
    summary_df = pd.DataFrame()
    
    states = _get_subdir(data_dump_path)

    
    for state in states:
        tpth = os.path.join(data_dump_path, state)

        # Each session has a sub directory
        sessions = _get_subdir(tpth)

        for session in sessions:
            # The filename starts with the relevant session year(s)
            # <year(s)>_<session_name>_[<session_id>]
            yrs = session.split('_')[0]
            session_id = int(re.findall("\[(.*?)\]", session)[0])
            
            tpth2 = os.path.join(tpth, session)
            
            # We assume that all sessions have the three folders bill, text, people
            bill_path = os.path.join(tpth2, 'bill')
            bill_files = os.listdir(bill_path) if os.path.isdir(bill_path) else []
            
            people_path = os.path.join(tpth2, 'people')
            people_files = os.listdir(people_path) if os.path.isdir(people_path) else [] 
            
            text_path = os.path.join(tpth2, 'text')
            text_files = os.listdir(text_path) if os.path.isdir(text_path) else [] 

            d = dict()
            d['state'] = state
            d['session_name'] = session
            d['session_id'] = session_id
            d['session_year(s)'] = yrs
            d['num_bills'] = len(bill_files)
            d['num_people'] = len(people_files)
            d['num_texts'] = len(text_files)

            summary_df = summary_df.append(d, ignore_index=True)

    summary_df.to_csv(save_target, index=False)
    print(summary_df)

            

if __name__ == '__main__':
    summarize_dump(
        data_dump_path=data_dump_path, 
        save_target=os.path.join(data_dump_path, 'data_dump_summary.csv')
    )