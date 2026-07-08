from __future__ import annotations

from collections import defaultdict

from kalshi_mlb_research.odds.implied_prob import no_vig_probabilities
from kalshi_mlb_research.odds.schemas import PregamePrior
from kalshi_mlb_research.time_utils import parse_iso_datetime, utc_now


def build_pregame_prior(event: dict, game_pk: str | None = None) -> PregamePrior | None:
    home_team = str(event.get("home_team") or "")
    away_team = str(event.get("away_team") or "")
    home_prices: list[int] = []
    away_prices: list[int] = []
    bookmaker_count = 0

    for bookmaker in event.get("bookmakers", []):
        h2h = next((market for market in bookmaker.get("markets", []) if market.get("key") == "h2h"), None)
        if not h2h:
            continue
        outcomes = {outcome.get("name"): outcome.get("price") for outcome in h2h.get("outcomes", [])}
        if home_team in outcomes and away_team in outcomes:
            home_prices.append(int(outcomes[home_team]))
            away_prices.append(int(outcomes[away_team]))
            bookmaker_count += 1

    if not home_prices or not away_prices:
        return None

    priors = [no_vig_probabilities(home, away) for home, away in zip(home_prices, away_prices)]
    home_prior = sum(home for home, _ in priors) / len(priors)
    away_prior = sum(away for _, away in priors) / len(priors)
    return PregamePrior(
        game_pk=game_pk,
        home_team=home_team,
        away_team=away_team,
        home_no_vig_prior=home_prior,
        away_no_vig_prior=away_prior,
        bookmaker_count=bookmaker_count,
        home_moneyline_range=(min(home_prices), max(home_prices)),
        away_moneyline_range=(min(away_prices), max(away_prices)),
        observed_at_utc=utc_now(),
    )

