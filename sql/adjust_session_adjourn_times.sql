-- The scraped session start and end times are not always reliable
-- There are some instances where bill activity persists beyond the said adjourn date
-- The fix:
-- If there is a session where activity exists (using bill_events table) beyond the ajourn date
-- we take the last year's max event date (for that state regular session) and use that as the adjourn date
-- We create a new table in the pre_triage_features schema

drop table if exists pre_triage_features.ajusted_session_dates;

create table pre_triage_features.ajusted_session_dates as 
-- Sessions that have unreliable adjourn dates (unreliable := there are events beyond the adjourn date)
with ncsl as (
	select  
		b.state,
		session_year,
		max(n.session_id) as session_id,
		max(adjourn_date) as adjourn_date,
		max(event_date) as last_event
	from clean.bills b left join clean.ncsl_legiscan_linked n
	on b.session_id=n.session_id
	join clean.bill_events e
	on b.bill_id=e.bill_id and (e.event_date > n.adjourn_date) and extract(year from e.event_date)=n.session_year
	where not special
	group by 1, 2
),
-- make sure that we only consider bills (and, their events) in regular sessions
regular_bills as (
	select 
		b1.*
	from clean.bills b1 join clean.sessions using(session_id)
	where not special
),
-- We take the events in the session from last 4 years and use the latest date as the adjourn date for this year
imputed_dates as (
	select 
		n2.state, 
		session_year,
		max(n2.session_id) as session_id,
		max(n2.session_year::text || '-' || to_char(event_date, 'MM-DD'))::date as new_adjourn_date
	from ncsl n2 join regular_bills b
	on n2.state=b.state
	join clean.bill_events e 
	on b.bill_id=e.bill_id and extract(year from e.event_date) between n2.session_year-4 and n2.session_year-1
	group by 1, 2
)
select 
	n1.session_id,
	n1.session_year,
	n1.state_name,
	n1.year_start,
	n1.year_end,
	n1.convene_date,
	case when (n1.adjourn_date > n3.new_adjourn_date) or n3.new_adjourn_date is null then n1.adjourn_date else n3.new_adjourn_date end as adjourn_date
from clean.ncsl_legiscan_linked n1 left join imputed_dates n3 using(session_id, session_year)

