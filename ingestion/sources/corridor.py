"""Corridor waypoint definitions and polygon-based proximity filter.

Used to restrict crash records (and other point data) to locations actually
along the I-80, US-50, HWY-88, and HWY-89 corridors. Polygon files live in
data/coords/ as GeoJSON-style JSON (one file per highway).
"""
from __future__ import annotations

import json
import math
from pathlib import Path

# ---------------------------------------------------------------------------
# Polygon loading — data/coords/*.json
# ---------------------------------------------------------------------------

_COORDS_DIR = Path(__file__).parent.parent.parent / "data" / "coords"

_POLYGON_FILES = {
    "I-80":    "i80_polygon.json",
    "HWY-88":  "hwy88_polygon.json",
    "HWY-89":  "hwy89_polygon.json",
    "US-50":   "us50_polygon.json",
}

# Loaded once at import: {highway_label: ring} where ring = [[lon, lat], ...]
_HIGHWAY_POLYGONS: dict[str, list[list[float]]] = {}

def _load_polygons() -> None:
    for label, fname in _POLYGON_FILES.items():
        path = _COORDS_DIR / fname
        if path.exists():
            data = json.loads(path.read_text())
            _HIGHWAY_POLYGONS[label] = data["coordinates"][0]

_load_polygons()


# ---------------------------------------------------------------------------
# Waypoints — used for BQ SQL generation and proximity fallback
# Each entry: (lat, lon, highway_label)
# ---------------------------------------------------------------------------

_LABELED_WAYPOINTS: list[tuple[float, float, str]] = [
    # I-80 (Sacramento → Truckee)
    (38.5816, -121.4944, "I-80"),   # Sacramento
    (38.7521, -121.2880, "I-80"),   # Roseville
    (38.8966, -121.0769, "I-80"),   # Auburn
    (39.1002, -120.9533, "I-80"),   # Colfax
    (39.2835, -120.6715, "I-80"),   # Emigrant_Gap
    (39.3232, -120.3253, "I-80"),   # Donner_Summit
    (39.3280, -120.1833, "I-80"),   # Truckee
    # US-50
    (38.7296, -120.7985, "US-50"),  # Placerville
    (38.8235, -120.0352, "US-50"),  # Echo_Summit
    (38.9399, -119.9772, "US-50"),  # South_Lake_Tahoe
    # HWY-88
    (38.3490, -120.7752, "HWY-88"), # Jackson
    (38.4330, -120.5707, "HWY-88"), # Pioneer
    (38.7054, -120.1024, "HWY-88"), # Carson_Spur
    (38.6868, -120.0657, "HWY-88"), # Kirkwood
    (38.6940, -119.9800, "HWY-88"), # Carson_Pass
    # HWY-89 (South Lake Tahoe → Truckee, west shore of Lake Tahoe)
    (38.9500, -119.9800, "HWY-89"), # South Lake Tahoe / HWY-89 junction
    (39.1706, -120.1471, "HWY-89"), # Tahoe City
    (39.2388, -120.0272, "HWY-89"), # Kings Beach area
    (39.3218, -120.2050, "HWY-89"), # Truckee / HWY-89 junction
]

# Backward-compat flat list used by bq_corridor_filter_sql
CORRIDOR_WAYPOINTS: list[tuple[float, float]] = [(lat, lon) for lat, lon, _ in _LABELED_WAYPOINTS]

CORRIDOR_RADIUS_KM = 8.0
# Fallback proximity radius when a point is outside all polygons but very close to a waypoint.
# Tighter than the BQ filter (8 km) to reduce false classifications at corridor junctions.
_FALLBACK_RADIUS_KM = 5.0


# ---------------------------------------------------------------------------
# Geometry helpers
# ---------------------------------------------------------------------------

def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(a))


def _point_in_ring(lat: float, lon: float, ring: list[list[float]]) -> bool:
    """Ray-casting point-in-polygon test.

    ring is [[lon, lat], ...] (GeoJSON coordinate order).
    Returns True if (lat, lon) is inside the polygon ring.
    """
    x, y = lon, lat
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def classify_hwy(lat: float, lon: float) -> str | None:
    """Return the highway label for (lat, lon).

    Primary: polygon containment test against data/coords/ polygons.
    Fallback: nearest waypoint within _FALLBACK_RADIUS_KM (handles points that
    lie on the road centerline but just outside the narrow corridor polygon).
    Returns None if outside all tracked corridors.
    """
    if _HIGHWAY_POLYGONS:
        for label in ("I-80", "US-50", "HWY-88", "HWY-89"):
            ring = _HIGHWAY_POLYGONS.get(label)
            if ring and _point_in_ring(lat, lon, ring):
                return label

        # Polygon miss — try nearest labeled waypoint within fallback radius
        best_dist, best_label = float("inf"), None
        for wp_lat, wp_lon, label in _LABELED_WAYPOINTS:
            d = _haversine_km(lat, lon, wp_lat, wp_lon)
            if d < best_dist:
                best_dist, best_label = d, label
        if best_dist <= _FALLBACK_RADIUS_KM:
            return best_label
        return None

    # No polygons available — pure proximity fallback
    best_dist, best_label = float("inf"), None
    for wp_lat, wp_lon, label in _LABELED_WAYPOINTS:
        d = _haversine_km(lat, lon, wp_lat, wp_lon)
        if d < best_dist:
            best_dist, best_label = d, label
    return best_label if best_dist <= CORRIDOR_RADIUS_KM else None


def is_near_corridor(lat: float, lon: float, radius_km: float = CORRIDOR_RADIUS_KM) -> bool:
    """True if (lat, lon) is inside any tracked corridor polygon.

    Falls back to waypoint proximity when polygon files are unavailable.
    """
    if _HIGHWAY_POLYGONS:
        return classify_hwy(lat, lon) is not None

    return any(
        _haversine_km(lat, lon, wp_lat, wp_lon) <= radius_km
        for wp_lat, wp_lon in CORRIDOR_WAYPOINTS
    )


def bq_corridor_filter_sql(lat_col: str = "lat", lon_col: str = "lon", radius_m: float = 8000.0) -> str:
    """BigQuery SQL fragment that returns TRUE if the row is near any corridor waypoint.

    Note: ST_DWITHIN takes radius in meters, not km.
    Used in dbt models to filter the crashes mart to corridor-only records.
    """
    clauses = [
        f"ST_DWITHIN(ST_GEOGPOINT({lon_col}, {lat_col}), ST_GEOGPOINT({lon:.4f}, {lat:.4f}), {radius_m})"
        for lat, lon in CORRIDOR_WAYPOINTS
    ]
    return "(\n  " + "\n  OR ".join(clauses) + "\n)"
