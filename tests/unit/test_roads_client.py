"""Unit tests for roads_client using mocked HTTP."""
from __future__ import annotations

import responses as responses_lib

from ingestion.sources.roads_client import ROADS_URL, fetch_road_conditions


@responses_lib.activate
def test_fetch_road_conditions_returns_text() -> None:
    html = (
        "<html><body>"
        "<p>I-80 chains required at Donner Summit</p>"
        "<td>Check conditions before driving</td>"
        "</body></html>"
    )
    responses_lib.add(responses_lib.GET, ROADS_URL, body=html, status=200, content_type="text/html")

    result = fetch_road_conditions("80")

    assert isinstance(result, str)
    assert len(result) > 0


@responses_lib.activate
def test_fetch_road_conditions_joins_elements_with_pipe() -> None:
    html = "<html><body><p>First</p><p>Second</p></body></html>"
    responses_lib.add(responses_lib.GET, ROADS_URL, body=html, status=200, content_type="text/html")

    result = fetch_road_conditions("80")

    assert "|" in result


@responses_lib.activate
def test_fetch_road_conditions_empty_elements_falls_back_to_get_text() -> None:
    html = "<html><body>Plain text with no block elements</body></html>"
    responses_lib.add(responses_lib.GET, ROADS_URL, body=html, status=200, content_type="text/html")

    result = fetch_road_conditions("80")

    assert isinstance(result, str)


@responses_lib.activate
def test_fetch_road_conditions_different_road_number() -> None:
    html = "<p>US-50 open</p>"
    responses_lib.add(responses_lib.GET, ROADS_URL, body=html, status=200, content_type="text/html")

    result = fetch_road_conditions("50")

    assert isinstance(result, str)
    assert len(result) > 0
