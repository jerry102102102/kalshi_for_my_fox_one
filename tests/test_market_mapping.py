from __future__ import annotations

from kalshi_mlb_research.kalshi.market_discovery import map_market_to_game
from kalshi_mlb_research.mlb.team_mapping import team_similarity


def test_team_name_mapping_similarity() -> None:
    assert team_similarity("New York Yankees", "Yankees") >= 0.95
    assert team_similarity("Boston Red Sox", "Red Sox") >= 0.95


def test_market_to_game_mapping() -> None:
    mapping = map_market_to_game(
        {"ticker": "TEST", "title": "Will New York Yankees beat Boston Red Sox?"},
        {"game_pk": "123", "home_team": "New York Yankees", "away_team": "Boston Red Sox"},
        threshold=0.70,
    )

    assert mapping is not None
    assert mapping.game_pk == "123"
    assert mapping.kalshi_ticker == "TEST"

