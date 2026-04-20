-- Tile 2 (temporal) data source — one row per segment per hour.
-- Incremental so dbt run on a live pipeline only appends new hours.
-- Streamlit queries this directly for the time-series chart.

{{
  config(
    unique_key = ['segment_id', 'window_hour'],
  )
}}

with windows as (
    select * from {{ ref('int_segment_windows') }}
    -- incremental: only process hours we haven't seen yet
    {% if is_incremental() %}
    where window_hour > (select max(window_hour) from {{ this }})
    {% endif %}
)

select
    segment_id,
    segment_name,
    window_hour,
    round(avg_score_hr, 1)    as avg_score,
    min_score_hr               as min_score,
    dominant_band,
    snowfall_events,
    chain_control_activations
from windows
order by segment_id, window_hour
