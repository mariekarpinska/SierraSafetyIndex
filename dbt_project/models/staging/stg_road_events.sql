/*
  stg_road_events
  purpose: cast + rename raw BQ strings → typed staging layer, no filtering
  source:  sierra_safety.raw_road_events (spark writes here every ~5 min)
  outputs: int_segment_windows, mart_safety_score_history
  note:    all columns come in as strings from spark's BQ writer, hence all the casts
*/

with source as (
    -- grab everything; filtering/deduplication happens downstream in int layer
    select * from {{ source('sierra_safety', 'raw_road_events') }}
),

renamed as (
    select
        segment_id,            -- SEG_01 thru SEG_15, already a string, no cast needed
        segment_name,          -- human label e.g. "Donner_Summit", for display only
        cast(lat as float64)           as lat,               -- BQ writes numerics as strings
        cast(lon as float64)           as lon,
        cast(elevation_ft as int64)    as elevation_ft,      -- feet, not meters
        cast(event_timestamp as timestamp) as event_timestamp,  -- UTC, from kafka message time
        cast(score as int64)           as score,             -- 0-100, higher = safer
        band,                                                -- GREEN/YELLOW/ORANGE/RED/BLACK, derived in scorer
        chain_control,                                       -- R1/R2/R3/None, kept raw for mart joins
        cast(road_closed as bool)      as road_closed,       -- hard override, score forced to 0 upstream
        cast(snowfall_rate_in_hr as float64) as snowfall_rate_in_hr,  -- inches/hr from open-meteo
        cast(visibility_miles as float64)    as visibility_miles,     -- from RWIS sensor or NWS fallback
        cast(wind_gust_mph as float64)       as wind_gust_mph,
        cast(surface_temp_c as float64)      as surface_temp_c,       -- celsius; negative = ice risk
        cast(seismic_mag as float64)         as seismic_mag           -- richter; null if no quake within 80km
    from source
)

select * from renamed
