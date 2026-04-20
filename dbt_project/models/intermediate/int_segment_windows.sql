-- Hourly window aggregates per segment. Intermediate layer between staging
-- and the mart — keeps the marts simple and the aggregation logic in one place.
--
-- dominant_band falls back to recalculating from avg_score if the stg_safety_scores
-- table doesn't have a row for that hour (e.g. backfill data doesn't populate it).

with road_events as (
    select * from {{ ref('stg_road_events') }}
),

safety_scores as (
    select * from {{ ref('stg_safety_scores') }}
),

-- pull segment metadata from the events table rather than a separate seed
-- so we don't have to maintain a separate static file
segment_meta as (
    select distinct
        segment_id,
        segment_name,
        lat,
        lon,
        elevation_ft
    from road_events
),

-- truncate to hour — producer runs every 5 min so each hour has ~12 events
hourly_events as (
    select
        segment_id,
        timestamp_trunc(event_timestamp, hour) as window_hour,
        avg(score)           as avg_score_hr,
        min(score)           as min_score_hr,
        max(score)           as max_score_hr,
        countif(snowfall_rate_in_hr > 0) as snowfall_events,
        countif(chain_control is not null and chain_control != 'None') as chain_control_activations,
        count(*)             as event_count
    from road_events
    group by 1, 2
),

joined as (
    select
        he.segment_id,
        sm.segment_name,
        sm.lat,
        sm.lon,
        sm.elevation_ft,
        he.window_hour,
        he.avg_score_hr,
        he.min_score_hr,
        he.max_score_hr,
        he.snowfall_events,
        he.chain_control_activations,
        he.event_count,
        -- prefer the spark-computed band; fall back to deriving from avg_score
        coalesce(
            ss.dominant_band,
            case
                when he.avg_score_hr >= 80 then 'GREEN'
                when he.avg_score_hr >= 60 then 'YELLOW'
                when he.avg_score_hr >= 40 then 'ORANGE'
                when he.avg_score_hr >= 20 then 'RED'
                else 'BLACK'
            end
        ) as dominant_band
    from hourly_events he
    left join segment_meta sm using (segment_id)
    left join safety_scores ss
        on he.segment_id = ss.segment_id
        and he.window_hour = timestamp_trunc(ss.window_start, hour)
)

select * from joined
