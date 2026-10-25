"""Report a job's result to Dagster when Dagster launched it.

Jobs stay plain command-line programs. Launched by the orchestrator through
Dagster Pipes, they report their metrics as the asset's materialization
metadata; run by hand, reporting does nothing.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any

Report = Callable[[Mapping[str, Any]], None]


@contextmanager
def materialization_reporter() -> Iterator[Report]:
    from dagster_pipes import PipesEnvVarParamsLoader, open_dagster_pipes

    if not PipesEnvVarParamsLoader().is_dagster_pipes_process():
        yield lambda metadata: None
        return
    with open_dagster_pipes() as pipes:
        yield lambda metadata: pipes.report_asset_materialization(metadata=dict(metadata))
