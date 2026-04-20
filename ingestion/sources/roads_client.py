"""Caltrans roads.dot.ca.gov scraper for I-80 road conditions."""
from __future__ import annotations

import requests
import structlog
from bs4 import BeautifulSoup

log = structlog.get_logger()

ROADS_URL = "https://roads.dot.ca.gov/roadscell.php"


def fetch_road_conditions(road_number: str = "80") -> str:
    """Fetch current road condition text from roads.dot.ca.gov.

    Returns raw text describing current I-80 conditions.
    """
    params = {"roadnumber": road_number}
    resp = requests.get(ROADS_URL, params=params, timeout=15)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text_parts: list[str] = []
    for elem in soup.find_all(["p", "td", "div", "span"]):
        text = elem.get_text(separator=" ", strip=True)
        if text:
            text_parts.append(text)
    raw = " | ".join(text_parts) if text_parts else soup.get_text(separator=" ", strip=True)
    log.info("road_conditions_fetched", road=road_number, length=len(raw))
    return raw
