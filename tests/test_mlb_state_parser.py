from __future__ import annotations

from kalshi_mlb_research.mlb.event_normalizer import SportsEventNormalizer
from kalshi_mlb_research.mlb.state_parser import MLBStateParser
from kalshi_mlb_research.sample_data import sample_mlb_live_payload


def test_parse_live_state_fields() -> None:
    state = MLBStateParser().parse(sample_mlb_live_payload("123"))

    assert state.game_pk == "123"
    assert state.inning == 7
    assert state.half_inning == "bottom"
    assert state.home_score == 4
    assert state.away_score == 3
    assert state.outs == 1
    assert state.balls == 2
    assert state.strikes == 1
    assert state.runner_on_first is True
    assert state.runner_on_second is False
    assert state.runner_on_third is True


def test_event_normalizer_detects_run_scored() -> None:
    parser = MLBStateParser()
    before = parser.parse(sample_mlb_live_payload("123"))
    payload = sample_mlb_live_payload("123")
    payload["liveData"]["linescore"]["teams"]["home"]["runs"] = 5
    after = parser.parse(payload)

    event = SportsEventNormalizer().normalize(before, after)

    assert event.event_type == "RUN_SCORED"

