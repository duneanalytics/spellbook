"""Render checks for the Dune table property post-hook macros.

These macros only emit SQL when `target.name == 'prod'`, so dbt CI (which runs against a `ci`
target) never renders them and a mistake would only surface during a production run. The tests
render the macros directly against a stubbed dbt context and assert on the properties they emit.
"""

import json
import re
from pathlib import Path
from types import SimpleNamespace

import pytest
from jinja2 import DictLoader, Environment
from jinja2.ext import do

MACRO_DIR = Path(__file__).resolve().parent.parent / "dune"
MACRO_FILES = (
    "config_trino_properties.sql",
    "mark_as_spell.sql",
    "private_data_explorer.sql",
    "expose_dataset.sql",
    "deprecate_spells.sql",
)
FILTERING_COLUMNS = "dune.data_explorer.filtering_columns"

# Every macro that emits Dune table properties, with representative arguments.
PROPERTY_MACROS = {
    "mark_as_spell": ("hive.tokens_evm.transfers", "view"),
    "expose_spells": ('["ethereum"]', "sector", "tokens", '["jeff-dude"]'),
    "hide_spells": (),
    "private_data_explorer": ('["ethereum"]', "sector", "tokens"),
    "expose_dataset": ('["ethereum"]', '["jeff-dude"]'),
    "deprecate_spells": (),
}

PROPERTY_PATTERN = re.compile(r"ROW\('([^']+)', '(.*?)'\)")


class CompilerError(Exception):
    """Stands in for dbt's compilation error, which is raised out of the macro context."""


def render(macro, materialized="view", config=None, target="prod"):
    source = "\n".join((MACRO_DIR / name).read_text() for name in MACRO_FILES)
    env = Environment(loader=DictLoader({"macros": source}), extensions=[do])
    model_config = {"materialized": materialized, **(config or {})}

    env.filters["as_text"] = lambda value: value
    env.globals.update(
        tojson=json.dumps,
        fromjson=json.loads,
        var=lambda name, default=None: "1h",
        exceptions=SimpleNamespace(
            raise_compiler_error=lambda message, node=None: (_ for _ in ()).throw(
                CompilerError(message)
            )
        ),
        target=SimpleNamespace(name=target),
        this="hive_catalog_svc.tokens_evm.transfers",
        model=SimpleNamespace(
            database="hive_catalog_svc",
            schema="tokens_evm",
            alias="transfers",
            config=SimpleNamespace(materialized=materialized, get=model_config.get),
        ),
    )
    template = env.get_template("macros")
    return getattr(template.module, macro)(*PROPERTY_MACROS[macro])


def emitted_properties(sql):
    return dict(PROPERTY_PATTERN.findall(sql))


@pytest.mark.parametrize("macro", PROPERTY_MACROS)
@pytest.mark.parametrize("materialized", ["view", "table", "incremental"])
def test_filtering_columns_emitted_by_every_property_macro(macro, materialized):
    sql = render(
        macro, materialized, {"filtering_columns": ["block_month", "blockchain"]}
    )
    assert json.loads(emitted_properties(sql)[FILTERING_COLUMNS]) == [
        "block_month",
        "blockchain",
    ]


@pytest.mark.parametrize("macro", PROPERTY_MACROS)
def test_filtering_columns_omitted_when_not_configured(macro):
    assert FILTERING_COLUMNS not in emitted_properties(render(macro))


@pytest.mark.parametrize("macro", PROPERTY_MACROS)
def test_empty_list_is_emitted_so_a_published_hint_can_be_cleared(macro):
    # Views only upsert the keys they are sent, so omitting the property would strand the old value.
    sql = render(macro, config={"filtering_columns": []})
    assert json.loads(emitted_properties(sql)[FILTERING_COLUMNS]) == []


@pytest.mark.parametrize(
    "value",
    [
        "block_month",  # bare string
        ["block_month", 3],  # non-string member
        (
            "block_month",
            3,
        ),  # same, as a tuple: %-formatting the error message would raise
        {
            "block_month": True
        },  # mapping: iterates as its keys, would serialize as an object
        5,
        True,
    ],
)
def test_invalid_filtering_columns_are_rejected(value):
    with pytest.raises(CompilerError, match="Invalid filtering_columns"):
        render("expose_spells", config={"filtering_columns": value})


@pytest.mark.parametrize("macro", PROPERTY_MACROS)
def test_nothing_is_emitted_outside_prod(macro):
    sql = render(macro, config={"filtering_columns": ["block_month"]}, target="ci")
    assert sql.strip() == ""


def test_views_and_tables_use_their_own_property_statement():
    assert "alter_view_properties" in render("expose_spells", "view")
    assert "ALTER TABLE" in render("expose_spells", "incremental")


def test_filtering_columns_does_not_displace_the_other_properties():
    sql = render("expose_spells", "incremental", {"filtering_columns": ["block_month"]})
    properties = emitted_properties(sql)
    # The table path rebuilds the whole metadata struct per statement, so a partial map drops fields.
    assert {
        "dune.created_by",
        "dune.public",
        "dune.visible",
        "dune.data_explorer.category",
        "dune.data_explorer.contributors",
        "dune.vacuum",
        FILTERING_COLUMNS,
    } <= set(properties)
