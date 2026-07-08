from __future__ import annotations

from kalshi_mlb_research.time_utils import utc_now


def sample_orderbook_payload() -> dict:
    return {
        "orderbook_fp": {
            "yes_dollars": [["0.42", "10"], ["0.41", "8"], ["0.40", "20"]],
            "no_dollars": [["0.55", "12"], ["0.54", "10"], ["0.53", "20"]],
        }
    }


def sample_mlb_live_payload(game_pk: str = "demo-game") -> dict:
    return {
        "gamePk": game_pk,
        "gameData": {
            "status": {"detailedState": "In Progress"},
            "teams": {"home": {"name": "New York Yankees"}, "away": {"name": "Boston Red Sox"}},
            "datetime": {"officialDate": utc_now().date().isoformat()},
        },
        "liveData": {
            "linescore": {
                "currentInning": 7,
                "inningHalf": "Bottom",
                "teams": {"home": {"runs": 4}, "away": {"runs": 3}},
                "offense": {
                    "first": {"id": 1, "fullName": "Runner One"},
                    "second": {},
                    "third": {"id": 3, "fullName": "Runner Three"},
                },
            },
            "plays": {
                "currentPlay": {
                    "about": {"inning": 7, "halfInning": "bottom", "endTime": utc_now().isoformat()},
                    "count": {"balls": 2, "strikes": 1, "outs": 1},
                    "matchup": {"batter": {"id": 10}, "pitcher": {"id": 20}},
                    "result": {"eventType": "single", "description": "Batter singles on a line drive."},
                }
            },
        },
    }

