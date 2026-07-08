from __future__ import annotations

from dataclasses import dataclass


def base_out_key(
    runner_on_first: bool,
    runner_on_second: bool,
    runner_on_third: bool,
    outs: int,
) -> str:
    bases = "".join(
        [
            "1" if runner_on_first else "0",
            "1" if runner_on_second else "0",
            "1" if runner_on_third else "0",
        ]
    )
    return f"{bases}_{outs}"


DEFAULT_RUN_EXPECTANCY = {
    "000_0": 0.54,
    "100_0": 0.93,
    "010_0": 1.17,
    "001_0": 1.43,
    "110_0": 1.55,
    "101_0": 1.85,
    "011_0": 2.02,
    "111_0": 2.31,
    "000_1": 0.29,
    "100_1": 0.55,
    "010_1": 0.70,
    "001_1": 0.98,
    "110_1": 0.94,
    "101_1": 1.21,
    "011_1": 1.42,
    "111_1": 1.55,
    "000_2": 0.11,
    "100_2": 0.24,
    "010_2": 0.33,
    "001_2": 0.38,
    "110_2": 0.46,
    "101_2": 0.54,
    "011_2": 0.59,
    "111_2": 0.76,
}


@dataclass(frozen=True)
class RunExpectancyTable:
    values: dict[str, float] | None = None

    def __post_init__(self) -> None:
        if self.values is None:
            object.__setattr__(self, "values", DEFAULT_RUN_EXPECTANCY.copy())

    def expected_runs(
        self,
        runner_on_first: bool,
        runner_on_second: bool,
        runner_on_third: bool,
        outs: int,
    ) -> float:
        key = base_out_key(runner_on_first, runner_on_second, runner_on_third, min(max(outs, 0), 2))
        return float(self.values.get(key, 0.0))

