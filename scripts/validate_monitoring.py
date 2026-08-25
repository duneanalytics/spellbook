#!/usr/bin/env python3
"""Validate `meta.monitoring` declarations against a parsed dbt manifest.

The manifest contains the merged values consumed by the monitoring writer. dbt does
not type-check or spell-check values under `meta`, so malformed declarations would
otherwise be silently omitted from the generated config table.

A monitored model must also declare dbt's native `event_time` config and document
that column. This script is stdlib-only so every subproject can run it in CI.
"""

from __future__ import annotations

import argparse
import difflib
import json
import sys
from pathlib import Path

MONITORING_KEY = "monitoring"
EVENT_TIME_KEY = "event_time"
ALLOWED_FIELDS = ("enabled", "warn_after", "critical_after", "oncall")
ENABLED_REQUIRED_FIELDS = ("warn_after", "critical_after", "oncall")
BOOL_FIELDS = ("enabled", "oncall")
THRESHOLD_FIELDS = ("warn_after", "critical_after")
THRESHOLD_KEYS = ("count", "period")
PERIOD_SECONDS = {"minute": 60, "hour": 3600, "day": 86400}
# Existing misspelled metadata keys score <= 0.571; plausible monitoring typos score >= 0.947.
TYPO_CUTOFF = 0.8


def load_models(manifest_path: Path) -> dict[str, dict]:
    """Return model nodes from a parsed manifest, keyed by unique ID."""
    try:
        with manifest_path.open(encoding="utf-8") as handle:
            manifest = json.load(handle)
    except FileNotFoundError:
        raise SystemExit(
            f"manifest not found at {manifest_path} -- run `dbt parse` first"
        )
    return {
        uid: node
        for uid, node in manifest.get("nodes", {}).items()
        if node.get("resource_type") == "model"
    }


def model_label(uid: str, node: dict) -> str:
    """Return `schema.alias (path)`, identifying the declaration for its owner."""
    config = node.get("config") or {}
    schema = config.get("schema")
    alias = config.get("alias") or node.get("name")
    relation = f"{schema}.{alias}" if schema and alias else uid.split(".")[-1]
    return f"{relation} ({node.get('original_file_path', uid)})"


def meta_of(node: dict) -> dict:
    meta = (node.get("config") or {}).get("meta")
    return meta if isinstance(meta, dict) else {}


def is_positive_int(value: object) -> bool:
    # bool is a subclass of int; `count: true` must not pass as 1.
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def threshold_seconds(threshold: dict) -> int:
    return int(threshold["count"]) * PERIOD_SECONDS[threshold["period"]]


def check_threshold(field: str, value: object, add: callable) -> bool:
    """Validate one {count, period} pair. Return whether it is safe to compare."""
    if not isinstance(value, dict):
        add(f"monitoring.{field} must be a mapping of {{count, period}}, got {value!r}")
        return False

    unknown = sorted(set(value) - set(THRESHOLD_KEYS))
    if unknown:
        add(
            f"monitoring.{field} has unknown key(s) {unknown}; "
            f"allowed: {list(THRESHOLD_KEYS)}"
        )
    missing = [key for key in THRESHOLD_KEYS if key not in value]
    if missing:
        add(f"monitoring.{field} is missing required key(s) {missing}")

    valid = not unknown and not missing
    if "count" in value and not is_positive_int(value["count"]):
        add(
            f"monitoring.{field}.count must be a positive integer, "
            f"got {value['count']!r}"
        )
        valid = False
    if "period" in value and value["period"] not in PERIOD_SECONDS:
        add(
            f"monitoring.{field}.period must be one of "
            f"{sorted(PERIOD_SECONDS)}, got {value['period']!r}"
        )
        valid = False
    return valid


def check_relation(node: dict, add: callable) -> None:
    """Validate fields required by `_monitoring_relation`."""
    schema = (node.get("config") or {}).get("schema")
    if not schema or schema == "no_schema":
        add(
            "monitoring.enabled is true but the model declares no production `schema`; "
            "the monitoring writer cannot construct delta_prod.<schema>.<table>"
        )


def check_event_time(node: dict, add: callable) -> None:
    """Require a monitored model to declare and document its event time."""
    config = node.get("config") or {}
    event_time = config.get(EVENT_TIME_KEY)
    if not event_time or not isinstance(event_time, str):
        add(
            "monitoring.enabled is true but the model declares no `event_time` config. "
            "Freshness is measured against event time; add config(event_time='<column>') "
            "naming the column that says when the event happened, never a write/load "
            "timestamp (a write time advances on every rewrite, so it reads fresh "
            "through the staleness being monitored)"
        )
        return

    columns = node.get("columns") or {}
    if not columns:
        add(
            f"monitored model documents no columns, so event_time {event_time!r} cannot "
            "be verified; document at least that column in the model's schema yml"
        )
        return
    if event_time.lower() not in {name.lower() for name in columns}:
        add(
            f"event_time {event_time!r} is not among the model's declared columns "
            f"({sorted(columns)}); document the column the alerting keys on, or fix the name. "
            "This reads the model's schema yml, not the built relation, so it cannot confirm "
            "the column exists -- only that it is documented"
        )


def validate_node(uid: str, node: dict) -> list[str]:
    """Return every problem with one model's monitoring declaration."""
    problems: list[str] = []
    add = problems.append
    meta = meta_of(node)

    for key in sorted(meta):
        if key == MONITORING_KEY:
            continue
        if difflib.SequenceMatcher(None, key.lower(), MONITORING_KEY).ratio() >= TYPO_CUTOFF:
            add(
                f"meta key {key!r} looks like a typo of {MONITORING_KEY!r}; dbt accepts "
                "any meta key silently, so the declaration under it is never read"
            )

    monitoring = meta.get(MONITORING_KEY)
    if monitoring is None:
        return problems
    if not isinstance(monitoring, dict):
        add(f"meta.monitoring must be a mapping, got {monitoring!r}")
        return problems

    unknown = sorted(set(monitoring) - set(ALLOWED_FIELDS))
    if unknown:
        add(
            f"meta.monitoring has unknown key(s) {unknown}; "
            f"allowed: {list(ALLOWED_FIELDS)}"
        )

    if "enabled" not in monitoring:
        add(
            "meta.monitoring is missing `enabled`; a declared block must say whether "
            "the table is monitored (true) or deliberately opted out (false)"
        )
    elif monitoring["enabled"] is True:
        incomplete = [field for field in ENABLED_REQUIRED_FIELDS if field not in monitoring]
        if incomplete:
            add(
                "meta.monitoring.enabled is true but the block is missing "
                f"{incomplete}; a monitored model must declare all of "
                f"{list(ENABLED_REQUIRED_FIELDS)}"
            )

    for field in BOOL_FIELDS:
        if field in monitoring and not isinstance(monitoring[field], bool):
            add(f"monitoring.{field} must be a boolean, got {monitoring[field]!r}")

    comparable = True
    for field in THRESHOLD_FIELDS:
        if field in monitoring:
            comparable &= check_threshold(field, monitoring[field], add)
        else:
            comparable = False

    if comparable:
        warn = threshold_seconds(monitoring["warn_after"])
        critical = threshold_seconds(monitoring["critical_after"])
        if critical <= warn:
            add(
                f"monitoring.critical_after ({critical}s) must be greater than "
                f"warn_after ({warn}s), or the warn tier can never fire first"
            )

    if monitoring.get("enabled") is True:
        check_relation(node, add)
        check_event_time(node, add)
    return problems


def validate_manifest(manifest_path: Path) -> int:
    models = load_models(manifest_path)
    findings = {
        uid: problems
        for uid, node in models.items()
        if (problems := validate_node(uid, node))
    }
    if not findings:
        declared = sum(1 for node in models.values() if MONITORING_KEY in meta_of(node))
        print(
            f"meta.monitoring OK: {declared} of {len(models)} models declare a block "
            "(the rest have none, which is a valid state)."
        )
        return 0

    print(
        f"meta.monitoring invalid on {len(findings)} of {len(models)} models:\n",
        file=sys.stderr,
    )
    for uid in sorted(findings, key=lambda value: model_label(value, models[value])):
        print(f"  {model_label(uid, models[uid])}", file=sys.stderr)
        for problem in findings[uid]:
            print(f"      - {problem}", file=sys.stderr)
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("target/manifest.json"),
        help="path to the parsed dbt manifest (default: target/manifest.json)",
    )
    return validate_manifest(parser.parse_args(argv).manifest)


if __name__ == "__main__":
    sys.exit(main())
