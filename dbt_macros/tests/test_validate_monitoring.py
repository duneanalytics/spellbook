"""Tests for the manifest-level `meta.monitoring` validation gate."""

from __future__ import annotations

import ast
import contextlib
import copy
import importlib.util
import io
import json
import re
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = ROOT / "scripts/validate_monitoring.py"
MACRO_PATH = ROOT / "dbt_macros/dune/monitoring_config.sql"
spec = importlib.util.spec_from_file_location("validate_monitoring", MODULE_PATH)
validate_monitoring = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validate_monitoring)

VALID_MONITORING = {
    "enabled": True,
    "warn_after": {"count": 6, "period": "hour"},
    "critical_after": {"count": 12, "period": "hour"},
    "oncall": False,
}


def node(
    monitoring=...,
    *,
    meta_extra=None,
    event_time="block_time",
    columns=("block_time", "amount"),
    schema="s",
):
    meta = {"sector": "dex"}
    if monitoring is not ...:
        meta["monitoring"] = monitoring
    meta.update(meta_extra or {})
    config = {"schema": schema, "alias": "a", "meta": meta}
    if event_time is not None:
        config["event_time"] = event_time
    return {
        "name": "y",
        "resource_type": "model",
        "original_file_path": "models/x/y.sql",
        "config": config,
        "columns": {name: {"name": name} for name in columns},
    }


def problems(monitoring=..., **kwargs):
    return validate_monitoring.validate_node(
        "model.spellbook.y", node(monitoring, **kwargs)
    )


def write_manifest(directory: str, nodes: dict) -> Path:
    path = Path(directory) / "manifest.json"
    path.write_text(json.dumps({"nodes": nodes}), encoding="utf-8")
    return path


def run_gate(nodes: dict) -> tuple[int, str]:
    with tempfile.TemporaryDirectory() as tmp:
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = validate_monitoring.validate_manifest(write_manifest(tmp, nodes))
    return code, err.getvalue()


def test_fully_specified_block_passes():
    assert problems(copy.deepcopy(VALID_MONITORING)) == []


def test_absent_block_is_skipped():
    assert problems() == []


def test_disabled_block_needs_no_thresholds_or_event_time():
    assert problems({"enabled": False}, event_time=None, columns=(), schema=None) == []


@pytest.mark.parametrize("period", ["minute", "hour", "day"])
def test_every_period_is_accepted(period):
    block = copy.deepcopy(VALID_MONITORING)
    block["warn_after"] = {"count": 1, "period": period}
    block["critical_after"] = {"count": 2, "period": period}
    assert problems(block) == []


def test_typo_fires_when_real_block_is_absent():
    found = problems(meta_extra={"monitorring": {"enabled": True}})
    assert any("looks like a typo" in problem for problem in found)


def test_real_spellbook_meta_keys_are_not_flagged_as_typos():
    siblings = {
        "dune": {},
        "sector": "x",
        "short_description": "x",
        "blockchain": "x",
        "docs_slug": "x",
        "contributors": [],
        "contributor": "x",
        "contibutors": "x",
        "pproject": "x",
    }
    assert problems(copy.deepcopy(VALID_MONITORING), meta_extra=siblings) == []


def test_declared_block_requires_enabled():
    found = problems({"oncall": True})
    assert len(found) == 1
    assert "missing `enabled`" in found[0]


def test_enabled_block_requires_every_field():
    found = problems({"enabled": True})
    assert any("enabled is true but the block is missing" in problem for problem in found)
    for field in ("warn_after", "critical_after", "oncall"):
        assert any(field in problem for problem in found)


def test_unknown_monitoring_key_is_rejected():
    block = {**VALID_MONITORING, "page_after": {"count": 1, "period": "day"}}
    assert any("unknown key(s) ['page_after']" in problem for problem in problems(block))


def test_unknown_threshold_key_is_rejected():
    block = copy.deepcopy(VALID_MONITORING)
    block["warn_after"]["unit"] = "hour"
    assert any("unknown key(s) ['unit']" in problem for problem in problems(block))


@pytest.mark.parametrize("bad", ["true", 1, None])
def test_enabled_must_be_boolean(bad):
    block = {**VALID_MONITORING, "enabled": bad}
    assert any("enabled must be a boolean" in problem for problem in problems(block))


def test_oncall_must_be_boolean():
    block = {**VALID_MONITORING, "oncall": "false"}
    assert any("oncall must be a boolean" in problem for problem in problems(block))


def test_monitoring_and_thresholds_must_be_mappings():
    assert any("must be a mapping" in problem for problem in problems("enabled"))
    block = {**VALID_MONITORING, "warn_after": "6h"}
    assert any("warn_after must be a mapping" in problem for problem in problems(block))


@pytest.mark.parametrize("bad", [0, -1, 1.5, "6", None, True])
def test_count_must_be_a_positive_integer(bad):
    block = copy.deepcopy(VALID_MONITORING)
    block["warn_after"] = {"count": bad, "period": "hour"}
    assert any(
        "count must be a positive integer" in problem for problem in problems(block)
    )


@pytest.mark.parametrize("bad", ["hours", "week", "second", "Hour", 3600])
def test_period_must_use_supported_vocabulary(bad):
    block = copy.deepcopy(VALID_MONITORING)
    block["warn_after"] = {"count": 6, "period": bad}
    assert any("period must be one of" in problem for problem in problems(block))


@pytest.mark.parametrize("critical", [6, 1])
def test_critical_must_be_greater_than_warn(critical):
    block = copy.deepcopy(VALID_MONITORING)
    block["critical_after"] = {"count": critical, "period": "hour"}
    assert any("must be greater than" in problem for problem in problems(block))


def test_ordering_is_compared_across_periods():
    block = copy.deepcopy(VALID_MONITORING)
    block["warn_after"] = {"count": 1, "period": "hour"}
    block["critical_after"] = {"count": 90, "period": "minute"}
    assert problems(block) == []
    block["critical_after"] = {"count": 30, "period": "minute"}
    assert any("must be greater than" in problem for problem in problems(block))


def test_monitored_model_requires_schema():
    for schema in (None, "", "no_schema"):
        found = problems(copy.deepcopy(VALID_MONITORING), schema=schema)
        assert any("declares no production `schema`" in problem for problem in found)


def test_monitored_model_requires_event_time():
    found = problems(copy.deepcopy(VALID_MONITORING), event_time=None)
    assert any("declares no `event_time` config" in problem for problem in found)
    assert any("write/load timestamp" in problem for problem in found)


def test_event_time_must_be_documented():
    found = problems(
        copy.deepcopy(VALID_MONITORING), event_time="block_time", columns=("amount",)
    )
    assert any("not among the model's declared columns" in problem for problem in found)


def test_event_time_column_match_is_case_insensitive():
    assert (
        problems(
            copy.deepcopy(VALID_MONITORING),
            event_time="Block_Time",
            columns=("block_time",),
        )
        == []
    )


def test_malformed_model_fails_the_manifest_gate():
    block = copy.deepcopy(VALID_MONITORING)
    block["critical_after"] = {"count": 1, "period": "hour"}
    code, err = run_gate({"model.spellbook.y": node(block)})
    assert code == 1
    assert "s.a (models/x/y.sql)" in err


def test_valid_manifest_passes_the_gate():
    code, err = run_gate(
        {"model.spellbook.y": node(copy.deepcopy(VALID_MONITORING))}
    )
    assert code == 0
    assert err == ""


def test_non_model_nodes_are_not_validated():
    with tempfile.TemporaryDirectory() as tmp:
        path = write_manifest(
            tmp,
            {
                "model.spellbook.y": node(copy.deepcopy(VALID_MONITORING)),
                "test.spellbook.not_null_y": {"resource_type": "test", "config": {}},
                "source.spellbook.raw.y": {"resource_type": "source", "config": {}},
            },
        )
        assert list(validate_monitoring.load_models(path)) == ["model.spellbook.y"]


def test_missing_manifest_is_a_clean_failure():
    with tempfile.TemporaryDirectory() as tmp:
        with pytest.raises(SystemExit, match="dbt parse"):
            validate_monitoring.load_models(Path(tmp) / "manifest.json")


@pytest.mark.parametrize(
    "block",
    [
        {
            "enabled": True,
            "critical_after": {"count": 12, "period": "hour"},
            "oncall": False,
        },
        {**VALID_MONITORING, "critical_after": "12h"},
        {**VALID_MONITORING, "warn_after": {"count": 6, "period": "week"}},
        {**VALID_MONITORING, "critical_after": {"count": "12", "period": "hour"}},
        {**VALID_MONITORING, "warn_after": {"count": 0, "period": "hour"}},
    ],
)
def test_every_block_the_macro_would_skip_is_rejected(block):
    assert problems(block)


def test_period_vocabulary_matches_the_macro():
    units = re.search(r"set units = (\{[^}]*\})", MACRO_PATH.read_text(encoding="utf-8"))
    assert units is not None
    assert ast.literal_eval(units.group(1)) == validate_monitoring.PERIOD_SECONDS
