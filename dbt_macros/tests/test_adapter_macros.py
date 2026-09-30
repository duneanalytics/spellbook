"""Render table DDL to verify defaults, model overrides, and target-specific properties."""

import datetime
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from jinja2 import Environment, StrictUndefined

ROOT = Path(__file__).resolve().parents[2]
PROJECTS = sorted((ROOT / "dbt_subprojects").glob("*/dbt_project.yml"))
SOURCE = (ROOT / "dbt_macros/dune/adapters.sql").read_text()


class Relation:
    schema = "example"
    identifier = "events"

    def __str__(self):
        return "hive.example.events"


def render(config=None, defaults=None, temporary=False, target="prod", on_exists="sql"):
    config = {} if config is None else config
    defaults = {"change_data_feed_enabled": "true"} if defaults is None else defaults
    original_config, original_defaults = deepcopy(config), deepcopy(defaults)
    module = Environment(
        extensions=["jinja2.ext.do"], undefined=StrictUndefined
    ).from_string(SOURCE).make_module({
        "config": config,
        "var": lambda name, fallback=None: {
            "dune_default_table_properties": defaults,
        }.get(name, fallback),
        "target": SimpleNamespace(
            name=target, database="dune" if target == "ci" else "hive",
            type="trino", schema="prod",
        ),
        "modules": SimpleNamespace(datetime=datetime),
        "local_md5": lambda value: "build_hash",
        "return": lambda value: value,
    })
    sql = module.trino__create_table_as(
        temporary, Relation(), "select 1 as id", on_exists
    )
    assert config == original_config
    assert defaults == original_defaults
    return sql


@pytest.mark.parametrize("project", PROJECTS, ids=lambda p: p.parent.name)
def test_each_subproject_enables_cdf_on_creation(project):
    defaults = yaml.safe_load(project.read_text())["vars"]["dune_default_table_properties"]
    sql = render(defaults=defaults, config={"partition_by": ["block_month"]})
    assert "change_data_feed_enabled = true" in sql
    assert "partitioned_by = ARRAY['block_month']" in sql


@pytest.mark.parametrize("config", [{}, {"properties": None}, {"properties": {}}])
def test_default_applies_without_model_properties(config):
    assert "change_data_feed_enabled = true" in render(config=config)


def test_model_properties_merge_with_defaults():
    sql = render(config={"properties": {"partitioned_by": "ARRAY['day']"}})
    assert "change_data_feed_enabled = true" in sql
    assert "partitioned_by = ARRAY['day']" in sql


def test_explicit_opt_out_preserves_other_defaults_and_partitioning():
    sql = render(
        config={
            "partition_by": ["block_month"],
            "properties": {"change_data_feed_enabled": "false"},
        },
        defaults={"change_data_feed_enabled": "true", "checkpoint_interval": "10"},
    )
    assert "change_data_feed_enabled = false" in sql
    assert "change_data_feed_enabled = true" not in sql
    assert "checkpoint_interval = 10" in sql
    assert "partitioned_by = ARRAY['block_month']" in sql


def test_explicit_partition_property_overrides_partition_by():
    sql = render(config={
        "partition_by": ["block_month"],
        "properties": {"partitioned_by": "ARRAY['day']"},
    })
    assert "partitioned_by = ARRAY['day']" in sql
    assert "ARRAY['block_month']" not in sql


def test_temporary_tables_keep_existing_partitioning_without_cdf():
    sql = render(temporary=True, config={
        "partition_by": ["block_month"],
        "properties": {"change_data_feed_enabled": "true"},
    })
    assert "change_data_feed_enabled" not in sql
    assert "partitioned_by = ARRAY['block_month']" in sql


@pytest.mark.parametrize("on_exists", ["sql", "replace"])
def test_creation_preserves_generated_location(on_exists):
    sql = render(on_exists=on_exists)
    assert "create or replace table hive.example.events" in sql
    assert "location = 's3a://prod-spellbook-trino-118330671040/example/events_build_hash'" in sql
    assert "change_data_feed_enabled = true" in sql


def test_ci_keeps_public_visibility_and_catalog_managed_location():
    sql = render(target="ci")
    assert "extra_properties = map_from_entries(ARRAY[ROW('dune.public', 'true')])" in sql
    assert "location =" not in sql
    assert "change_data_feed_enabled = true" in sql
