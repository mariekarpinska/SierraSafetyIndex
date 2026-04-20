/*
  stg_safety_scores
  purpose: cast spark-aggregated 15-min window rows → typed staging layer
  source:  sierra_safety.safety_scores (spark structured streaming writes here)
  outputs: mart_safety_score_history, mart_segment_risk_profile
  note:    already aggregated upstream by spark, not individual events
*/

with source as (
    -- one row per segment per 15-min tumbling window; no dedup needed
    select * from {{ source('sierra_safety', 'safety_scores') }}
),

renamed as (
    select
        cast(window_start as timestamp) as window_start,  -- start of 15-min window, UTC
        cast(window_end as timestamp)   as window_end,    -- always window_start + 15 min
        segment_id,                                        -- SEG_01 thru SEG_15
        cast(avg_score as float64)   as avg_score,        -- mean score across events in window
        cast(min_score as int64)     as min_score,        -- worst reading, drives alert logic
        cast(max_score as int64)     as max_score,        -- best reading in window
        cast(event_count as int64)   as event_count,      -- how many raw events landed in this window
        dominant_band                                      -- mode of band values across events
    from source
)

select * from renamed
