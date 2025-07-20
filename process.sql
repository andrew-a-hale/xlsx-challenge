with detect_header as (
  select
    *,
    concat_ws(',', *columns(* exclude (filepath, sheet, row))) as headers,
    case
      when headers like 'row_id%position' then 1
      when headers like 'row_id%emp_type' then 2
      when headers like 'first_name%hourly_rate' then 3
    end as header_type,
    sum(header_type = 1) over (partition by filepath, sheet order by row) as row_group_1,
    sum(header_type = 2) over (partition by filepath, sheet order by row) as row_group_2,
    sum(header_type = 3) over (partition by filepath, sheet order by row) as row_group_3
  from 'final.parquet'
)
-- row_id,full_name,dob,emp_start,date,emp_type,hourly_rate,position
select
  filepath,
  sheet,
  A as row_id,
  split(B, ' ')[2] as first_name,
  split(B, ',')[1] as last_name,
  C as dob,
  D as emp_start,
  E as date,
  F as emp_type,
  G as hourly_rate,
  H as position
from detect_header
where row_group_1 > 0 and F in ('emp_type', 'CASUAL', 'CONTRACT', 'PART_TIME', 'FULL_TIME', 'TEMPORARY')
union
-- row_id,full_name,dob,date,position,hourly_rate,emp_start,emp_type
select
  filepath,
  sheet,
  A as row_id,
  split(B, ' ')[1] as first_name,
  split(B, ' ')[2] as last_name,
  C as dob,
  G as emp_start,
  D as date,
  H as emp_type,
  F as hourly_rate,
  E as position
from detect_header
where row_group_2 > 0 and H in ('emp_type', 'CASUAL', 'CONTRACT', 'PART_TIME', 'FULL_TIME', 'TEMPORARY')
union
-- first_name,last_name,dob,row_id,emp_start,date,position,emp_type,hourly_rate
select
  filepath,
  sheet,
  D as row_id,
  A as first_name,
  B as last_name,
  C dob,
  E as emp_start,
  F as date,
  H as emp_type,
  I as hourly_rate,
  G as position
from detect_header
where row_group_3 > 0 and H in ('emp_type', 'CASUAL', 'CONTRACT', 'PART_TIME', 'FULL_TIME', 'TEMPORARY');
