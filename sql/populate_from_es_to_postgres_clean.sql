SET ROLE rg_staff;

-- populate clean.bill_docs
insert into clean.bill_docs
with not_duplicates as(
    select
        doc_id,
        bill_id,
        max(type) as type,
        min(doc_date) as doc_date,
        max(text_size) as text_size,
        max(mime_id) as mime_id,
        max(url) as url,
        max(state_link) as state_link
    from
       raw.bill_text_es
    group by
        doc_id,
        bill_id
)
select
    cast(doc_id as integer),
    cast(bill_id as integer),
    type as doc_type,
    doc_date,
    text_size,
    cast(mime_id as integer) as mime_id,
    url,
    state_link
from not_duplicates;

--populate clean.bills
insert into clean.bills
with bills_from_es as(
  select
    bill_id,
    session->'session_id' as session_id,
    bill_type,
    bill_number,
    state,
    title,
    description,
    subjects::jsonb,
    body as introduced_body,
    url,
    state_link
  from raw.bill_meta_es
),
introduced_dates as (
    select
        bill_id,
        min(cast(progress->>'date' as date)) as introduced_date
    from (
    	select 
    		bill_id,
    		jsonb_array_elements(progress::jsonb) as progress
    	from raw.bill_meta_es
    ) as t
    where progress->>'event' = '1'
    group by bill_id
)
select 
	cast(bill_id as integer),
    cast(session_id as integer),
    bill_type,
    bill_number,
    state,
    title,
    description,
    subjects,
    introduced_date,
    introduced_body,
    url,
    state_link
from bills_from_es left join introduced_dates using(bill_id);


--populate clean.sessions
insert into clean.sessions
 select
     cast(session->'session_id' as integer) as session_id,
     max(cast(session->>'session_title' as text)) as session_title,
     min(cast(session->'year_start' as integer)) as year_start,
     max(cast(session->'year_end' as integer)) as year_end,
     state_id as state_id,
     bool_or(cast(session->>'special' as boolean)) as special
 from raw.bill_meta_es
 group by
     session->'session_id',
     state_id;


--populate clean.bill_events
insert into clean.bill_events
with data_from_es as(
   select
        bill_id,
        jsonb_array_elements(history::jsonb) as events
   from raw.bill_meta_es
),
event_hash as(
    select
        cast(bill_id as integer) as bill_id,
        cast(events->>'date' as date) as event_date,
        events->>'action' as action,
        events->>'chamber' as chamber,
        cast(events->>'importance' as smallint) as important,
        md5(row(events->>'action', events->>'chamber', events->>'date')::text) as event_hash
    from data_from_es
),
selected_important as(
    select
        bill_id,
        max(important) as important,
        event_hash
    from event_hash
    group by
        bill_id,
        event_hash
)
select distinct on(
    bill_id,
    event_date,
    action,
    chamber,
    important,
    event_hash)
    bill_id,
    event_date,
    action,
    chamber,
    important,
    event_hash
from event_hash
join selected_important
using(bill_id, event_hash, important);

--populate clean.bills_sponsors
 insert into clean.bill_sponsors
 with data_from_es as(
   select
     bill_id,
     jsonb_array_elements(sponsors::jsonb) as sponsors
   from raw.bill_meta_es
 )
 select
     cast(sponsors->>'people_id' as integer) as sponsor_id,
     cast(bill_id as integer),
     max(cast(sponsors->>'party_id' as smallint)) as party_id,
     max(sponsors->>'role') as role,
     max(cast(sponsors->>'sponsor_type_id' as smallint)) as sponsor_type,
     min(cast(sponsors->>'sponsor_start_date' as date)) as sponsor_start_date,
     max(CASE when sponsors->>'sponsor_end_date'='null' then null else sponsors->>'sponsor_end_date' end)::date as sponsor_end_date
 from data_from_es
 group by
 1, 2;

--populate clean.bill_votes
insert into clean.bill_votes
with data_from_es as(
 select
    bill_id,
    jsonb_array_elements(votes::jsonb) as votes,
    url,
    state_link
 from raw.bill_meta_es
)
select
    cast(votes->>'roll_call_id' as integer) as vote_id,
    cast(bill_id as integer),
    cast(votes->>'date' as date) as vote_date,
    votes->>'desc' as description,
    cast(votes->>'yea' as smallint),
    cast(votes->>'nay' as smallint),
    cast(votes->>'nv' as smallint),
    cast(votes->>'absent' as smallint),
    cast(votes->>'total' as smallint),
    cast(votes->>'passed' as boolean),
    votes->>'chamber',
    url,
    state_link
from data_from_es;

--populate clean.session_people
insert into clean.session_people
select 
    cast(session_id as integer), 
    cast(people_id as integer), 
    person_hash, 
    state_id, 
    cast(party_id as smallint),
    party, 
    cast(role_id as integer), 
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
    openscrets_id as opensecrets_id, 
    ballotpedia,
    cast(committee_sponsor as boolean), 
    cast(committee_id as smallint)
from raw.session_people_es
where people_id is not null;

--amendments table
insert into clean.bill_amendments
with amendments as(
  select
    bill_id,
    jsonb_array_elements(amendments::jsonb) as amendments
  from raw.bill_meta_es
)
select
    cast(bill_id as integer),
    cast(amendments->>'amendment_id' as integer) as amendment_id,
    cast(amendments->>'date' as date) as amendment_date,
    amendments->>'chamber',
    cast(amendments->>'adopted' as smallint) as adopted,
    amendments->>'title' as amendment_title,
    amendments->>'description' as amendment_description,
    amendments->>'url',
    amendments->>'state_link'
from amendments;


--committee table
insert into clean.bill_committees
with committees as(
    select
        cast(bill_id as integer),
        cast(committee->'committee_id' as integer) as committee_id,
        committee->'chamber' as chamber,
        committee->'name' as name
    from raw.bill_meta_es
)
select
    bill_id,
    committee_id,
    chamber,
    name
from committees
where committee_id is not null
and chamber is not null
and name is not null
group by
    bill_id,
    committee_id,
    chamber,
    name;

--progress table
insert into clean.bill_progress
with progress as(
 select
    bill_id,
    jsonb_array_elements(progress::jsonb) as progress
 from raw.bill_meta_es
)

select
    cast(bill_id as integer) as bill_id,
    cast(progress->>'date' as date) as progress_date,
    cast(progress->>'event' as smallint) as event
from progress
group by
    bill_id,
    progress->>'date',
    progress->>'event';

-- --table clean.session_dates
-- insert into clean.session_dates
-- select

-- from raw.bill_meta_es

-- session_entry_id serial, -- not using the name session_id as it is already used in legiscan data
-- 	state_name varchar,
-- 	convene_date date,
-- 	adjourn_date date,
-- 	special boolean,
-- 	comments text


