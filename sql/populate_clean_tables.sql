--- This script is deprecated

--table clean.bill_amendments
insert into clean.bill_amendments
select cast(bill_id as integer), cast(amendment_id as integer), amendment_date, chamber, cast(adopted as smallint),
amendment_title, amendment_description, url, state_link
from raw.bill_amendments;

-- table clean.bill_committees
insert into clean.bill_committees
select cast(bill_id as integer), cast(committee_id as integer), chamber, name
from raw.bill_committees;

--table clean.bill_docs
insert into clean.bill_docs
select cast(doc_id as integer), cast(bill_id as integer), doc_type, doc_date, cast(text_size as integer)
from raw.bill_docs;

--table clean.bill_events
insert into clean.bill_events
select cast(bill_id as integer), event_date, action, chamber, cast(important as smallint)
from raw.bill_events;

--table clean.bill_progress
insert into clean.bill_progress
select cast(bill_id as integer), progress_date, cast(event as smallint)
from raw.bill_progress;

--table clean.bill_sponsors
insert into clean.bill_sponsors
select cast(sponsor_id as integer), cast(bill_id as integer), cast(party_id as smallint), role,
cast(sponsor_type as smallint)
from raw.bill_sponsors;

--table clean.bill_votes
insert into clean.bill_votes
select cast(vote_id as integer), cast(bill_id as integer), vote_Date, description, cast(yea as smallint),
cast(nay as smallint), cast(nv as smallint), cast(absent as smallint), cast(total as smallint),
cast(passed as boolean), chamber, url, state_link
from raw.bill_votes;

--table clean.bills
--there are 2 bills 421123 and 933606 that has all fields equal but subject...
insert into clean.bills
with bill_uniques as(
    select distinct bill_id, session_id, introduced_date, bill_type, bill_number, state_id, state, subjects,
    introduced_body, url
    from raw.bills
),

bill_introduced as(
    select bill_id, session_id, min(introduced_date) as introduced_date
    from bill_uniques
    group by bill_id, session_id
)

select cast(bill_id as integer), cast(session_id as integer), bill_type, bill_number, cast(state_id as integer), state, subjects, introduced_date,
introduced_body, url
from bill_introduced
join bill_uniques using(bill_id, session_id, introduced_date);




--table session_people
insert into clean.session_people
select cast(session_id as integer), cast(people_id as integer), person_hash, state_id, cast(party_id as smallint),
party, cast(role_id as integer), role, name, first_name, last_name, middle_name, suffix, nickname, district,
ftm_eid as ftm_id, votesmart_id, opensecrets_id, ballotpedia,
cast(committee_sponsor as boolean), cast(committee_id as smallint)
from raw.session_people_es;


--table sessions
insert into clean.sessions
select cast(session_id as integer), session_title, cast(year_start as smallint), cast(year_end as smallint),
cast(state_id as smallint), cast(special as boolean)
from raw.sessions;