# Constants used across the project

S3_BUCKET = 'aclu-leg-tracker'
PROJECT_FOLDER = '/mnt/data/projects/aclu_leg_tracker/'
BILL_TEXT_INDEX = "bill_text"
BILL_META_INDEX = "bill_meta"
ISSUE_REPRODUCTIVE_RIGHTS = "reproductive_rights"
ES_LABEL_SCHEMA = "labels_es"
ISSUE_CLASSIFIER_FEATURES_SCHEMA = 'issue_classifier_features'
SESSION_HASHES = 'dump_session_hashes'
SESSION_PEOPLE_INDEX = "session_people"
S3_BUCKET_LEGISCAN_UPDATES = 'weekly_updates_legiscan'

ISSUE_AREAS = ['criminal_justice', 'voting_rights', 'racial_justice', 'immigrants_rights', 'tech_privacy_civil_liberties', 'lgbtq_rights', 'other']
ISSUE_AREA_LABEL_SET = {'primary', 'major', 'minor'}

# The place holder used by legiscan for a missing date field. 
# We can't use this in elasticsearch. THerefore, we replace this with the DEFAULT DATE
LEGISCAN_PLACEHOLDER_DATE = '0000-00-00'

# The default data we use for any field with a missing date
# The missing dates need to be handled appropriately for downstream tasks
DEFAULT_DATE = '1970-01-01' 
