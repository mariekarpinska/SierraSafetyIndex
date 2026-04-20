"""Unit tests for scoring/safety_score.py — 12 required test cases."""
from __future__ import annotations

from scoring.safety_score import SegmentReading, ScoredReading, compute_score


def _reading(**kwargs) -> SegmentReading:
    return SegmentReading(segment_id="SEG_06", **kwargs)


def test_perfect_conditions() -> None:
    """No hazards → score == 100, band == GREEN."""
    result = compute_score(_reading())
    assert result.score == 100
    assert result.band == "GREEN"


def test_r2_chain_control() -> None:
    """R2 chain control → score == 70 (100 - 30)."""
    result = compute_score(_reading(chain_control="R2"))
    assert result.score == 70
    assert result.band == "YELLOW"


def test_r3_chain_control() -> None:
    """R3 chain control → score == 40 (100 - 60)."""
    result = compute_score(_reading(chain_control="R3"))
    assert result.score == 40
    assert result.band == "ORANGE"


def test_road_closure() -> None:
    """road_closed=True → score == 0, band == BLACK."""
    result = compute_score(_reading(road_closed=True))
    assert result.score == 0
    assert result.band == "BLACK"


def test_heavy_snowfall() -> None:
    """2 in/hr snowfall → score -= 40 (capped at 40)."""
    result = compute_score(_reading(snowfall_rate_in_hr=2.0))
    assert result.score == 60


def test_low_visibility() -> None:
    """0.2 miles visibility → score -= 25."""
    result = compute_score(_reading(visibility_miles=0.2))
    assert result.score == 75


def test_high_wind() -> None:
    """65 mph gusts → score -= 20."""
    result = compute_score(_reading(wind_gust_mph=65.0))
    assert result.score == 80


def test_freezing_surface() -> None:
    """surface_temp == -5°C → score -= 15 (black ice likely)."""
    result = compute_score(_reading(surface_temp_c=-5.0))
    assert result.score == 85


def test_significant_quake() -> None:
    """M4.2 within 80km → score -= 15."""
    result = compute_score(_reading(seismic_mag=4.2))
    assert result.score == 85


def test_combined_r2_snow_wind() -> None:
    """R2 + 1 in/hr snow + 45 mph wind → score == 100 - 30 - 20 - 10 == 40, band == ORANGE."""
    result = compute_score(
        _reading(chain_control="R2", snowfall_rate_in_hr=1.0, wind_gust_mph=45.0)
    )
    assert result.score == 40
    assert result.band == "ORANGE"


def test_score_floor() -> None:
    """Extreme combined inputs → score never below 0."""
    result = compute_score(
        _reading(
            chain_control="R3",
            snowfall_rate_in_hr=5.0,
            visibility_miles=0.1,
            wind_gust_mph=80.0,
            surface_temp_c=-10.0,
            seismic_mag=5.0,
        )
    )
    assert result.score >= 0
    assert result.score <= 100


def test_active_penalties_populated() -> None:
    """Combined scenario returns non-empty active_penalties list of strings."""
    result = compute_score(
        _reading(chain_control="R2", snowfall_rate_in_hr=1.0, wind_gust_mph=45.0)
    )
    assert isinstance(result.active_penalties, list)
    assert len(result.active_penalties) > 0
    assert all(isinstance(p, str) for p in result.active_penalties)


def test_red_band() -> None:
    """Score between 20–39 maps to RED band."""
    result = compute_score(
        _reading(chain_control="R2", snowfall_rate_in_hr=1.0, visibility_miles=0.3)
    )
    # 100 - 30 - 20 - 15 = 35 → RED
    assert result.score == 35
    assert result.band == "RED"


def test_r1_chain_control() -> None:
    """R1 chain control → score == 85 (100 - 15)."""
    result = compute_score(_reading(chain_control="R1"))
    assert result.score == 85


def test_moderate_visibility() -> None:
    """0.4 miles visibility → score -= 15."""
    result = compute_score(_reading(visibility_miles=0.4))
    assert result.score == 85


def test_limited_visibility() -> None:
    """0.7 miles visibility → score -= 8."""
    result = compute_score(_reading(visibility_miles=0.7))
    assert result.score == 92


def test_moderate_wind() -> None:
    """45 mph gusts → score -= 10."""
    result = compute_score(_reading(wind_gust_mph=45.0))
    assert result.score == 90


def test_elevated_wind() -> None:
    """35 mph gusts → score -= 5."""
    result = compute_score(_reading(wind_gust_mph=35.0))
    assert result.score == 95


def test_freezing_surface_mild() -> None:
    """surface_temp == -2°C (0 > temp >= -4) → score -= 8."""
    result = compute_score(_reading(surface_temp_c=-2.0))
    assert result.score == 92


def test_minor_quake() -> None:
    """M3.1 within 80km → score -= 5."""
    result = compute_score(_reading(seismic_mag=3.1))
    assert result.score == 95


def test_crash_penalty_fatal() -> None:
    """Fatal crash nearby → score -= 20."""
    result = compute_score(_reading(recent_crash_count=1, recent_crash_severity="Fatal"))
    assert result.score == 80
    assert any("crash" in p.lower() for p in result.active_penalties)


def test_crash_penalty_severe_injury() -> None:
    """Severe injury crash → score -= 12."""
    result = compute_score(_reading(recent_crash_count=1, recent_crash_severity="Severe Injury"))
    assert result.score == 88


def test_crash_penalty_visible_injury() -> None:
    """Other Visible Injury → score -= 6."""
    result = compute_score(_reading(recent_crash_count=1, recent_crash_severity="Other Visible Injury"))
    assert result.score == 94


def test_crash_penalty_complaint_of_pain() -> None:
    """Complaint of Pain → score -= 6 (via 'pain' in sev)."""
    result = compute_score(_reading(recent_crash_count=1, recent_crash_severity="Complaint of Pain"))
    assert result.score == 94


def test_crash_penalty_property_damage() -> None:
    """Property damage only with 2 crashes → score -= min(3*2, 9) = 6."""
    result = compute_score(_reading(recent_crash_count=2, recent_crash_severity="Property Damage Only"))
    assert result.score == 94


def test_crash_penalty_none_severity() -> None:
    """None severity with 1 crash → score -= min(3*1, 9) = 3."""
    result = compute_score(_reading(recent_crash_count=1, recent_crash_severity=None))
    assert result.score == 97


def test_zero_snowfall_no_penalty() -> None:
    """0 in/hr snowfall → no penalty (deduction == 0)."""
    result = compute_score(_reading(snowfall_rate_in_hr=0.0))
    assert result.score == 100
