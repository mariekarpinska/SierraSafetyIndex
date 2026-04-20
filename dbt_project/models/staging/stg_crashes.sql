-- Staged crash records: normalize primary_factor to human-readable categories.
-- Handles both old SWITRS (already mapped) and new CCRS text (VC codes + free text).

select
    case_id,
    collision_datetime,
    lat,
    lon,
    severity,
    collision_type,
    hwy,
    case
        when regexp_contains(upper(primary_factor), r'2235[0O]|UNSAFE SPEED')
            then 'Unsafe Speed'
        when regexp_contains(upper(primary_factor), r'22107|UNSAFE TURN')
            then 'Unsafe Turn / No Signal'
        when regexp_contains(upper(primary_factor), r'21658|UNSAFE LANE CHANGE|LANED ROADWAY')
            then 'Unsafe Lane Change'
        when regexp_contains(upper(primary_factor), r'23152|23153|DRIVING UNDER INFLUENCE|UNDER THE INFLUENCE')
            then 'DUI'
        when regexp_contains(upper(primary_factor), r'21453|STEADY CIRCULAR RED|RED LIGHT|RED ARROW')
            then 'Red Light Violation'
        when regexp_contains(upper(primary_factor), r'21804|ENTERING OR CROSSING')
            then 'Failure to Yield (Entering Hwy)'
        when regexp_contains(upper(primary_factor), r'21802|FAIL TO STOP AT STOP|STOP SIGN')
            then 'Failure to Yield (Stop Sign)'
        when regexp_contains(upper(primary_factor), r'21801')
            then 'Failure to Yield (Left Turn)'
        when regexp_contains(upper(primary_factor), r'21800|21803|FAIL.*YIELD|FAILURE.*YIELD')
            then 'Failure to Yield (Intersection)'
        when regexp_contains(upper(primary_factor), r'21703|FOLLOWING TOO CLOS')
            then 'Following Too Closely'
        when regexp_contains(upper(primary_factor), r'22106|UNSAFE START')
            then 'Unsafe Start from Stopped'
        when regexp_contains(upper(primary_factor), r'2175[0-9]|UNSAFE PASS')
            then 'Unsafe Passing'
        when regexp_contains(upper(primary_factor), r'21650|21651|WRONG SIDE|WRONG WAY')
            then 'Wrong Side of Road'
        when regexp_contains(upper(primary_factor), r'22450|STOP SIGN|FAIL TO STOP AT SIGN|FAILING TO STOP')
            then 'Stop Sign Violation'
        when regexp_contains(upper(primary_factor), r'UNSAFE BACK|BACKING')
            then 'Unsafe Backing'
        when upper(coalesce(primary_factor, '')) in ('UNKNOWN', 'OTHER', '')
            then 'Unknown'
        else 'Other'
    end as primary_factor,
    route
from {{ source('sierra_safety', 'crashes') }}
where collision_datetime is not null
  and lat is not null
  and lon is not null
  and hwy in ('I-80', 'US-50', 'HWY-88', 'HWY-89')
