"""Safety score computation for Sierra Nevada I-80 corridor segments.

Pure python — no I/O, no network calls, easy to unit test in isolation.
Producer calls compute_score() on every poll cycle; backfill calls it per
hourly row. Score range 0-100, higher = safer.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# all inputs are optional — sensors go offline, RWIS stations have gaps
@dataclass
class SegmentReading:
    """All sensor inputs for one corridor segment at a point in time."""

    segment_id: str
    chain_control: Optional[str] = None   # "None", "R1", "R2", "R3" from Caltrans CWWP2
    road_closed: Optional[bool] = None
    snowfall_rate_in_hr: Optional[float] = None
    visibility_miles: Optional[float] = None
    wind_gust_mph: Optional[float] = None
    surface_temp_c: Optional[float] = None
    seismic_mag: Optional[float] = None
    # from SWITRS/CCRS: crashes within ~10 mi of segment in last 6 hrs
    recent_crash_count: Optional[int] = None
    recent_crash_severity: Optional[str] = None  # "Fatal", "Severe Injury", etc.


@dataclass
class ScoredReading:
    """Scoring result for one corridor segment."""

    segment_id: str
    score: int
    band: str  # GREEN / YELLOW / ORANGE / RED / BLACK
    active_penalties: list[str] = field(default_factory=list)


# thresholds match the dashboard color bands
def _band(score: int) -> str:
    """Map numeric score to color band."""
    if score >= 80:
        return "GREEN"
    if score >= 60:
        return "YELLOW"
    if score >= 40:
        return "ORANGE"
    if score >= 20:
        return "RED"
    return "BLACK"


def compute_score(reading: SegmentReading) -> ScoredReading:
    """Compute safety score (0-100) from a SegmentReading.

    Starts at 100, subtracts for each hazard. Chain control is checked first
    since R3 alone usually puts you in RED/BLACK territory. Road closed is a
    hard override — skip the rest of scoring.
    """
    score = 100
    penalties: list[str] = []

    # hard override — don't even bother scoring other factors
    if reading.road_closed:
        return ScoredReading(
            segment_id=reading.segment_id,
            score=0,
            band="BLACK",
            active_penalties=["Road closed"],
        )

    # chain control: R3 = chains all vehicles, road often about to close
    cc = (reading.chain_control or "").upper()
    if cc == "R3":
        score -= 60
        penalties.append("R3 chain control (-60): chains required all vehicles")
    elif cc == "R2":
        score -= 30
        penalties.append("R2 chain control (-30): chains required except 4WD+snow tires")
    elif cc == "R1":
        score -= 15
        penalties.append("R1 chain control (-15): chains required except 4WD+snow tires w/ traction")

    # snowfall: scales linearly up to -40, caps at 2 in/hr
    if reading.snowfall_rate_in_hr is not None:
        deduction = min(40, int(reading.snowfall_rate_in_hr * 20))
        if deduction > 0:
            score -= deduction
            penalties.append(
                f"Snowfall {reading.snowfall_rate_in_hr:.1f} in/hr (-{deduction})"
            )

    if reading.visibility_miles is not None:
        if reading.visibility_miles < 0.25:
            score -= 25
            penalties.append(f"Low visibility {reading.visibility_miles:.2f} mi (-25)")
        elif reading.visibility_miles < 0.5:
            score -= 15
            penalties.append(f"Reduced visibility {reading.visibility_miles:.2f} mi (-15)")
        elif reading.visibility_miles < 1.0:
            score -= 8
            penalties.append(f"Limited visibility {reading.visibility_miles:.2f} mi (-8)")

    if reading.wind_gust_mph is not None:
        if reading.wind_gust_mph > 60:
            score -= 20
            penalties.append(f"Extreme wind gusts {reading.wind_gust_mph:.0f} mph (-20)")
        elif reading.wind_gust_mph > 40:
            score -= 10
            penalties.append(f"High wind gusts {reading.wind_gust_mph:.0f} mph (-10)")
        elif reading.wind_gust_mph > 30:
            score -= 5
            penalties.append(f"Elevated wind gusts {reading.wind_gust_mph:.0f} mph (-5)")

    # surface_temp from RWIS — black ice threshold is -4C, not 0C
    if reading.surface_temp_c is not None:
        if reading.surface_temp_c < -4:
            score -= 15
            penalties.append(
                f"Road surface {reading.surface_temp_c:.1f}°C (-15): black ice likely"
            )
        elif reading.surface_temp_c < 0:
            score -= 8
            penalties.append(
                f"Road surface {reading.surface_temp_c:.1f}°C (-8): freezing"
            )

    # seismic: anything >= M3 within 80 km is a rockfall/landslide risk on these mountain roads
    if reading.seismic_mag is not None:
        if reading.seismic_mag >= 4.0:
            score -= 15
            penalties.append(
                f"M{reading.seismic_mag:.1f} earthquake (-15): landslide/rockfall risk"
            )
        elif reading.seismic_mag >= 3.0:
            score -= 5
            penalties.append(f"M{reading.seismic_mag:.1f} earthquake (-5)")

    # crash severity drives penalty more than count — one fatal outweighs 10 fender benders
    if reading.recent_crash_count:
        sev = (reading.recent_crash_severity or "").lower()
        if "fatal" in sev:
            crash_pen = 20
        elif "severe" in sev:
            crash_pen = 12
        elif "visible" in sev or "pain" in sev:
            crash_pen = 6
        else:
            crash_pen = min(3 * reading.recent_crash_count, 9)
        score -= crash_pen
        penalties.append(
            f"{reading.recent_crash_count} recent crash(es) near segment "
            f"[{reading.recent_crash_severity}] (-{crash_pen})"
        )

    score = max(0, min(100, score))
    return ScoredReading(
        segment_id=reading.segment_id,
        score=score,
        band=_band(score),
        active_penalties=penalties,
    )
