"""CI must bound new sandwich tables without truncating production rebuilds."""

from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import Environment, StrictUndefined

MACRO_DIR = Path(__file__).resolve().parents[2] / "dbt_subprojects/dex/macros/models"


@pytest.mark.parametrize("name", ["dex_sandwiches", "dex_sandwiched"])
@pytest.mark.parametrize(
    "target,incremental,bounded",
    [("ci", False, True), ("ci", True, True), ("prod", False, False), ("prod", True, True)],
)
def test_input_time_bounds(name, target, incremental, bounded):
    env = Environment(undefined=StrictUndefined)
    env.globals.update(
        ref=lambda model: model,
        target=SimpleNamespace(name=target),
        is_incremental=lambda: incremental,
        var=lambda key, default=None: default,
        incremental_predicate=lambda column: f"{column} >= current_date - interval '3' day",
    )
    module = env.from_string((MACRO_DIR / f"{name}.sql").read_text()).module
    if name == "dex_sandwiches":
        sql = module.dex_sandwiches("bnb", "fixture_transactions")
        aliases = ["front", "back", "victim", "dt", "tx"]
    else:
        sql = module.dex_sandwiched("bnb", "fixture_transactions", "fixture_sandwiches")
        aliases = ["front", "back", "dt", "txs"]
    for alias in aliases:
        predicate = f"{alias}.block_time >= current_date - interval '3' day"
        assert (predicate in sql) == bounded, (name, target, incremental, alias)
