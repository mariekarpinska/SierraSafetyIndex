"""Unit tests for ingestion/sources/corridor.py."""
from __future__ import annotations

import pytest

from ingestion.sources.corridor import (
    CORRIDOR_WAYPOINTS,
    _HIGHWAY_POLYGONS,
    _haversine_km,
    bq_corridor_filter_sql,
    is_near_corridor,
)

_polygons_loaded = pytest.mark.skipif(
    not _HIGHWAY_POLYGONS,
    reason="data/coords/*.json polygon files not present (gitignored); "
    "polygon-containment tests are skipped on fresh clones.",
)


def test_haversine_same_point_is_zero() -> None:
    assert _haversine_km(39.3232, -120.3253, 39.3232, -120.3253) == 0.0


def test_haversine_donner_to_truckee_reasonable_distance() -> None:
    dist = _haversine_km(39.3232, -120.3253, 39.3280, -120.1833)
    assert 10.0 < dist < 20.0


def test_is_near_corridor_donner_summit() -> None:
    assert is_near_corridor(39.3232, -120.3253)


def test_is_near_corridor_false_for_remote_point() -> None:
    assert not is_near_corridor(36.0, -115.0)


def test_is_near_corridor_custom_radius_wide() -> None:
    assert is_near_corridor(39.3280, -120.1833, radius_km=200.0)


@_polygons_loaded
def test_is_near_corridor_custom_radius_tiny() -> None:
    # When corridor polygons are loaded, radius_km is ignored — inclusion is determined by
    # polygon containment. A point near Truckee is inside the I-80 polygon and returns True
    # regardless of the radius_km argument.
    assert is_near_corridor(39.3290, -120.1850, radius_km=0.001)


@_polygons_loaded
def test_is_near_corridor_outside_all_polygons() -> None:
    # A point in rural Nevada — outside all corridor polygons and beyond fallback radius
    assert not is_near_corridor(39.5, -118.0, radius_km=0.001)


def test_bq_corridor_filter_sql_returns_string() -> None:
    sql = bq_corridor_filter_sql()
    assert isinstance(sql, str)
    assert "ST_DWITHIN" in sql
    assert "ST_GEOGPOINT" in sql


def test_bq_corridor_filter_sql_custom_cols() -> None:
    sql = bq_corridor_filter_sql(lat_col="crash_lat", lon_col="crash_lon", radius_m=5000.0)
    assert "crash_lat" in sql
    assert "crash_lon" in sql
    assert "5000.0" in sql


def test_bq_corridor_filter_sql_has_all_waypoints() -> None:
    sql = bq_corridor_filter_sql()
    assert sql.count("ST_DWITHIN") == len(CORRIDOR_WAYPOINTS)
