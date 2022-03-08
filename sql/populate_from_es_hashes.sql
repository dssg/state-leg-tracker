--first time to get table hashes
insert into legiscan_update_metadata.bill_hashes
select bill_id, session->'session_id' as session_id, change_hash as bill_hash, current_date
from raw.bill_meta_es;