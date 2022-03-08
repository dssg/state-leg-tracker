-- We are joining the NCSL session dates and the legiscan sessions in this script
-- The "start_year" is used to do the join. 

set role rg_staff;

drop table if exists clean.ncsl_legiscan_linked;

create table if not exists clean.ncsl_legiscan_linked as (
	with legiscan as (
		select 
			session_id,
			year_start,
			year_end,
			state as state_name,
			special
		from clean.sessions join catalogs.states using (state_id)
		where not special
	),
	ncsl as (
		select 
			session_year,
			state_name,
			min(convene_date) as convene_date,
			max(adjourn_date) as adjourn_date,
			max(notes) as notes
		from raw.session_dates
		group by 1, 2
	)
	select 
		session_id,
		a.state_name,
		session_year,
		year_start,
		year_end,
		convene_date,
		adjourn_date,
		special,
		notes
	from legiscan a left join ncsl b
	on (year_start=session_year or year_end=session_year) and a.state_name = b.state_name
	where year_start >= 2011 or year_end >=2011)
