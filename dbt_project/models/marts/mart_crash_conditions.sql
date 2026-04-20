{{
  config(
    materialized = 'table',
    partition_by = {
      "field": "collision_datetime",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by = ["severity", "segment_id"]
  )
}}

-- crash records joined with road conditions at time of crash.
-- "what were the sensor readings when this crash happened?"
-- used by tile 3 (crash map) and tile 4 (conditions context) in dashboard.
--
-- nearest segment by euclidean distance — haversine overkill for ~300km corridor.
-- conditions match: latest road_event reading up to 2 hrs before crash timestamp.
-- if crashes table is empty (no CCRS data loaded yet) this materializes empty — dashboard handles it.

with crashes as (
    select
        case_id,
        collision_datetime,
        lat,
        lon,
        severity,
        collision_type,
        primary_factor,
        route,
        hwy
    from {{ ref('stg_crashes') }}
),

road_events as (
    select
        segment_id,
        segment_name,
        lat         as seg_lat,
        lon         as seg_lon,
        event_timestamp,
        score,
        band,
        chain_control,
        snowfall_rate_in_hr,
        visibility_miles,
        wind_gust_mph,
        surface_temp_c,
        seismic_mag
    from {{ ref('stg_road_events') }}
),

-- For each crash, find the nearest segment by approximate Euclidean distance
-- (sufficient for ~300km corridor at these latitudes)
crash_with_nearest_segment as (
    select
        c.case_id,
        c.collision_datetime,
        c.lat,
        c.lon,
        c.severity,
        c.collision_type,
        c.primary_factor,
        c.route,
        c.hwy,
        sm.segment_id,
        sm.segment_name,
        sm.seg_lat,
        sm.seg_lon,
        -- approximate distance in degrees
        sqrt(pow(c.lat - sm.seg_lat, 2) + pow(c.lon - sm.seg_lon, 2)) as approx_dist_deg
    from crashes c
    cross join (
        select distinct segment_id, segment_name, seg_lat, seg_lon
        from road_events
    ) sm
    qualify row_number() over (
        partition by c.case_id
        order by sqrt(pow(c.lat - sm.seg_lat, 2) + pow(c.lon - sm.seg_lon, 2))
    ) = 1
),

-- Join nearest road conditions (up to 2 hours before crash)
crash_conditions as (
    select
        cs.case_id,
        cs.collision_datetime,
        cs.lat,
        cs.lon,
        cs.severity,
        cs.collision_type,
        cs.primary_factor,
        cs.route,
        cs.hwy,
        cs.segment_id,
        cs.segment_name,
        cs.approx_dist_deg,
        re.score            as safety_score_at_crash,
        re.band             as safety_band_at_crash,
        re.chain_control,
        re.snowfall_rate_in_hr,
        re.visibility_miles,
        re.wind_gust_mph,
        re.surface_temp_c,
        re.seismic_mag,
        re.event_timestamp  as conditions_timestamp
    from crash_with_nearest_segment cs
    left join road_events re
        on  re.segment_id = cs.segment_id
        and re.event_timestamp between
                timestamp_sub(cs.collision_datetime, interval 2 hour)
                and cs.collision_datetime
    qualify row_number() over (
        partition by cs.case_id
        order by re.event_timestamp desc nulls last
    ) = 1
)

select * from crash_conditions
