from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime

from kalshi_mlb_research.mlb.schemas import MLBGameState
from kalshi_mlb_research.models.run_expectancy import RunExpectancyTable, base_out_key
from kalshi_mlb_research.time_utils import utc_now


def _clamp_probability(value: float) -> float:
    return min(0.995, max(0.005, value))


def _logit(p: float) -> float:
    p = _clamp_probability(p)
    return math.log(p / (1.0 - p))


def _sigmoid(value: float) -> float:
    return 1.0 / (1.0 + math.exp(-value))


@dataclass(frozen=True)
class ProbabilityPrediction:
    game_pk: str
    market_ticker: str | None
    observed_at_utc: datetime
    home_win_p_mid: float
    home_win_p_low: float
    home_win_p_high: float
    away_win_p_mid: float
    away_win_p_low: float
    away_win_p_high: float
    model_version: str
    features: dict = field(default_factory=dict)
    explanation: dict = field(default_factory=dict)


@dataclass
class MLBWinProbabilityModel:
    run_expectancy: RunExpectancyTable = field(default_factory=RunExpectancyTable)
    model_version: str = "baseline-logit-v1"

    def predict(
        self,
        state: MLBGameState,
        pregame_home_prior: float | None = None,
        market_ticker: str | None = None,
        stale_data_penalty: float = 0.0,
    ) -> ProbabilityPrediction:
        prior = 0.5 if pregame_home_prior is None else _clamp_probability(pregame_home_prior)
        inning = min(max(state.inning, 1), 12)
        outs = min(max(state.outs, 0), 2)
        score_diff = state.score_diff_home
        is_home_batting = state.is_home_batting
        re = self.run_expectancy.expected_runs(
            state.runner_on_first,
            state.runner_on_second,
            state.runner_on_third,
            outs,
        )
        late = 1 if inning >= 7 else 0
        extra = 1 if inning > 9 else 0
        score_weight = 0.42 + 0.12 * min(inning, 9)

        contributions = {
            "prior_logit": 0.85 * _logit(prior),
            "score_diff": score_weight * score_diff,
            "home_batting": (0.08 + 0.08 * re) if is_home_batting else (-0.08 * re),
            "outs": (-0.045 * outs) if is_home_batting else (0.035 * outs),
            "late_score": late * 0.18 * score_diff,
            "extra_inning_home": extra * 0.08,
        }
        z = sum(contributions.values())
        p_mid = _clamp_probability(_sigmoid(z))

        uncertainty = 0.045
        if pregame_home_prior is None:
            uncertainty += 0.04
        if inning <= 3:
            uncertainty += 0.025
        if late and abs(score_diff) <= 1:
            uncertainty += 0.035
        if re > 1.5:
            uncertainty += 0.02
        uncertainty += max(0.0, stale_data_penalty)
        uncertainty = min(0.22, uncertainty)

        p_low = max(0.0, p_mid - uncertainty)
        p_high = min(1.0, p_mid + uncertainty)
        features = {
            "pregame_home_prior": prior,
            "inning": inning,
            "half_inning": state.half_inning,
            "outs": outs,
            "balls": state.balls,
            "strikes": state.strikes,
            "runner_on_first": state.runner_on_first,
            "runner_on_second": state.runner_on_second,
            "runner_on_third": state.runner_on_third,
            "home_score_minus_away_score": score_diff,
            "is_home_batting": is_home_batting,
            "base_out_state": base_out_key(*state.base_state, outs),
            "base_out_expected_runs": re,
            "late_inning_flag": bool(late),
            "extra_inning_flag": bool(extra),
        }
        return ProbabilityPrediction(
            game_pk=state.game_pk,
            market_ticker=market_ticker,
            observed_at_utc=utc_now(),
            home_win_p_mid=p_mid,
            home_win_p_low=p_low,
            home_win_p_high=p_high,
            away_win_p_mid=1.0 - p_mid,
            away_win_p_low=max(0.0, 1.0 - p_high),
            away_win_p_high=min(1.0, 1.0 - p_low),
            model_version=self.model_version,
            features=features,
            explanation={"contributions_logit": contributions, "uncertainty": uncertainty},
        )

