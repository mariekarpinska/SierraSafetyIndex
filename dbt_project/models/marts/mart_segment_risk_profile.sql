-- All-time risk profile per segment — one row per segment, full table rebuild.
-- Answers "how dangerous is this segment historically?" rather than "what is it now?"
-- Ordered by elevation desc so highest/most dangerous segments appear first.
-- safe_divide handles segments with 0 total_windows (shouldn't happen but BQ throws on /0).

{{ config(materialized='table') }}

with windows as (
    select * from {{ ref('int_segment_windows') }}
),

road_events as (
    select * from {{ ref('stg_road_events') }}
),

segment_scores as (
    select
        segment_id,
        segment_name,
        elevation_ft,
        count(*) as total_windows,
        countif(dominant_band in ('RED', 'BLACK'))  as red_black_windows,
        countif(dominant_band = 'ORANGE')           as orange_windows,
        countif(dominant_band = 'YELLOW')           as yellow_windows,
        countif(dominant_band = 'GREEN')            as green_windows,
        avg(avg_score_hr)                           as avg_score_all_time
    from windows
    group by 1, 2, 3
),

-- worst single reading across all time — useful for "how bad did it get?"
worst_event as (
    select
        segment_id,
        min(score)          as worst_recorded_score,
        min_by(event_timestamp, score) as worst_recorded_at
    from road_events
    group by 1
)

select
    ss.segment_id,
    ss.segment_name,
    ss.elevation_ft,
    safe_divide(ss.red_black_windows, ss.total_windows) as pct_time_red_or_black,
    safe_divide(ss.orange_windows, ss.total_windows)    as pct_time_orange,
    safe_divide(ss.yellow_windows, ss.total_windows)    as pct_time_yellow,
    safe_divide(ss.green_windows, ss.total_windows)     as pct_time_green,
    round(ss.avg_score_all_time, 1)                     as avg_score_all_time,
    we.worst_recorded_score,
    we.worst_recorded_at
from segment_scores ss
left join worst_event we using (segment_id)
order by ss.elevation_ft desc
