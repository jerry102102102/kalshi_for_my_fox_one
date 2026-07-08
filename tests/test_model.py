from __future__ import annotations

from dataclasses import replace

from kalshi_mlb_research.mlb.state_parser import MLBStateParser
from kalshi_mlb_research.models.baseline_mlb_wp import MLBWinProbabilityModel
from kalshi_mlb_research.sample_data import sample_mlb_live_payload


def test_late_home_lead_has_high_home_probability() -> None:
    state = MLBStateParser().parse(sample_mlb_live_payload("123"))
    state = replace(state, inning=9, half_inning="bottom", home_score=5, away_score=3)

    prediction = MLBWinProbabilityModel().predict(state, pregame_home_prior=0.5)

    assert prediction.home_win_p_mid > 0.90

