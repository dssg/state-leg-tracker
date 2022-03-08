
--populate raw.bill_docs
insert into raw.bill_docs
select doc_id, bill_id, type, doc_date, text_size
from raw.bill_text_es;

--populate raw.bills
with step_1 as(
  select bill_id, session->'session_id' as session_id, bill_type, bill_number, state,
  jsonb_array_elements(subjects::jsonb) as subjects,
  jsonb_array_elements(progress::jsonb) as progress,
  body as introduced_body,url
  from raw.bill_meta_es
)
insert into raw.bills
select bill_id, session_id, bill_type, bill_number, state_id, state, subjects->>'subject_name' as subjects,
cast(progress->>'date' as date) as introduced_date, introduced_body, url
from step_1
where progress->>'event' = '1'
;


--populate raw.sessions
insert into raw.sessions
select session->'session_id', session->'session_title', session->'year_start', session->'year_end', state_id,
session->'special'
from raw.bill_meta_es;


--populate raw.bills_events
with step_1 as(
   select bill_id,
   jsonb_array_elements(history::jsonb) as events
   from raw.bill_meta_es
)

insert into raw.bill_events
select bill_id, cast(events->>'date' as date) as event_date,
events->>'action' as action, events->>'chamber' as chamber, events->>'importance' as important
from step_1;

--populate raw.bills_sponsors
with step_1 as(
  select bill_id,
  jsonb_array_elements(sponsors::jsonb) as sponsors
  from raw.bill_meta_es
)

insert into raw.bill_sponsors
select sponsors->>'people_id' as sponsor_id, bill_id, sponsors->>'party_id', sponsors->>'role',
sponsors->>'sponsor_type_id' as sponsor_type
from step_1;

--populate raw.bill_votes
with step_1 as(
 select bill_id, url, state_link,
 jsonb_array_elements(votes::jsonb) as votes
 from raw.bill_meta_es
)

insert into raw.bill_votes
select votes->>'roll_call_id' as vote_id, bill_id, cast(votes->>'date' as date) as vote_date,
votes->>'desc' as votes_description, votes->>'yea', votes->>'nay', votes->>'nv', votes->>'absent',
votes->>'total', votes->>'passed', votes->>'chamber', url, state_link
from step_1;

--committee table
insert into raw.bill_committees
select bill_id, committee->'committee_id' as committee_id, committee->'chamber' as chamber,
committee->'name' as name
from raw.bill_meta_es;

--Deprecated. Populating from new elasticsearch index now
--session_people
with step_1 as(
  select session->'session_id' as session_id, jsonb_array_elements(sponsors::jsonb) as sponsors,
  state_id
  from raw.bill_meta_es
)

insert into raw.session_people
select session_id, sponsors->>'people_id' as people_id, sponsors->>'person_hash', state_id,
sponsors->>'party_id', sponsors->>'party', sponsors->>'role_id', sponsors->>'role', sponsors->>'name',
sponsors->>'first_name', sponsors->>'last_name', sponsors->>'middle_name', sponsors->>'suffix',
sponsors->>'nickname', sponsors->>'district', sponsors->>'ftm_eid', sponsors->>'votesmart_id',
sponsors->>'opensecrets_id', sponsors->>'ballotpedia', sponsors->>'committee_sponsor',
sponsors->>'committee_id'
from step_1;

--amendments table
with step_1 as(
  select bill_id, jsonb_array_elements(amendments::jsonb) as amendments
  from raw.bill_meta_es
)

insert into raw.bill_amendments
select bill_id, amendments->>'amendment_id', cast(amendments->>'date' as date) as amendment_date,
amendments->>'chamber', amendments->>'adopted', amendments->>'title' as amendment_title,
amendments->>'description' as amendment_description, amendments->>'url', amendments->>'state_link'
from step_1;


--progress table
with step_1 as(
 select bill_id,
 jsonb_array_elements(progress::jsonb) as progress
 from raw.bill_meta_es
)

insert into raw.bill_progress
select bill_id, cast(progress->>'date' as date) as event_date, progress->>'event'
from step_1;