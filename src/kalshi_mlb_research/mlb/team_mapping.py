from __future__ import annotations

import re
from difflib import SequenceMatcher

ALIASES = {
    "arizona diamondbacks": "diamondbacks",
    "atlanta braves": "braves",
    "baltimore orioles": "orioles",
    "boston red sox": "red sox",
    "chicago cubs": "cubs",
    "chicago white sox": "white sox",
    "cincinnati reds": "reds",
    "cleveland guardians": "guardians",
    "colorado rockies": "rockies",
    "detroit tigers": "tigers",
    "houston astros": "astros",
    "kansas city royals": "royals",
    "los angeles angels": "angels",
    "los angeles dodgers": "dodgers",
    "miami marlins": "marlins",
    "milwaukee brewers": "brewers",
    "minnesota twins": "twins",
    "new york mets": "mets",
    "new york yankees": "yankees",
    "athletics": "athletics",
    "philadelphia phillies": "phillies",
    "pittsburgh pirates": "pirates",
    "san diego padres": "padres",
    "san francisco giants": "giants",
    "seattle mariners": "mariners",
    "st louis cardinals": "cardinals",
    "st. louis cardinals": "cardinals",
    "tampa bay rays": "rays",
    "texas rangers": "rangers",
    "toronto blue jays": "blue jays",
    "washington nationals": "nationals",
}


def normalize_team_name(value: str) -> str:
    lowered = value.lower().replace("&", " and ")
    lowered = re.sub(r"[^a-z0-9\s.]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return ALIASES.get(lowered, lowered)


def team_similarity(left: str, right: str) -> float:
    left_norm = normalize_team_name(left)
    right_norm = normalize_team_name(right)
    if not left_norm or not right_norm:
        return 0.0
    if left_norm == right_norm:
        return 1.0
    if left_norm in right_norm or right_norm in left_norm:
        return 0.95
    return SequenceMatcher(None, left_norm, right_norm).ratio()

