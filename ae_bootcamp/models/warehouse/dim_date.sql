select 
  format_date('%F', d)  as date_id,
  d                     as full_date,
  extract(YEAR from d)  as year,
  extract(WEEK from d)  as year_week,
  extract(DAY from d)   as year_day,
  extract(YEAR from d)  as fiscal_year,
  format_date('%Q', d)  as fiscal_quarter,
  extract(MONTH from d) as month,
  format_date('%B', d)  as month_name,
  format_date('%w', d)  as week_day,
  format_date('%A', d)  as day_name,
  case when format_date('%A', d) in ('Saturday', 'Sunday') then 1 else 0 end as day_is_weekend
from
  (
  select *
  from unnest(generate_date_array('2014-01-01', '2050-01-01', interval 1 day)) as d
  )