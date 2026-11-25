"""Real dbt view/incremental builds against an isolated Trino memory catalog."""

import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import yaml
from trino.dbapi import connect


ROOT = Path(__file__).resolve().parents[2]
MACROS = ROOT / "dbt_macros/dune"


@unittest.skipUnless(os.environ.get("COLUMN_COMMENTS_TRINO_PORT"), "isolated Trino not configured")
class TrinoColumnCommentsTest(unittest.TestCase):
    def test_native_builds_and_model_only_backfill(self):
        with tempfile.TemporaryDirectory(prefix="dbt-comments-") as directory:
            project = Path(directory)
            schema = project.name.replace("-", "_")
            (project / "models").mkdir()
            (project / "macros").mkdir()
            for name in ("column_comments.sql", "backfill_column_comments.sql"):
                (project / "macros" / name).symlink_to(MACROS / name)
            (project / "dbt_project.yml").write_text(yaml.safe_dump({
                "name": "comment_test", "version": "1.0", "config-version": 2,
                "profile": "comment_test", "model-paths": ["models"], "macro-paths": ["macros"],
                "models": {"comment_test": {"+persist_docs": {"columns": True}}},
            }))
            (project / "profiles.yml").write_text(yaml.safe_dump({"comment_test": {
                "target": "local", "outputs": {"local": {
                    "type": "trino", "method": "none", "user": "comment_test",
                    "host": "localhost", "port": int(os.environ["COLUMN_COMMENTS_TRINO_PORT"]),
                    "http_scheme": "http", "database": "memory", "schema": schema, "threads": 1,
                }},
            }}))
            description = "Sender's project ID -- path \\ snow ☃;\nnext"
            columns = [
                {"name": "ID", "description": "Identifier"},
                {"name": "from", "description": description},
                {"name": "Owner", "quote": True, "description": "Quoted owner"},
                {"name": "amount", "description": ""},
            ]
            for name, config in (
                ("fixture_view", "materialized='view'"),
                ("fixture_incremental", "materialized='incremental', incremental_strategy='append'"),
            ):
                (project / "models" / f"{name}.sql").write_text(
                    "{{ config(" + config + ") }}\n"
                    "select 7 as id, 'abc' as \"from\", 11 as \"Owner\", 3 as amount\n")
            (project / "models/schema.yml").write_text(yaml.safe_dump({
                "version": 2, "models": [
                    {"name": name, "columns": columns}
                    for name in ("fixture_view", "fixture_incremental")],
            }, allow_unicode=True))
            (project / "macros/check_unchanged.sql").write_text("""
{% macro check_unchanged() %}
  {% for name in ['fixture_view', 'fixture_incremental'] %}
    {% set node = graph.nodes['model.comment_test.' ~ name] %}
    {% set relation = adapter.get_relation(database=node.database, schema=node.schema, identifier=node.alias) %}
    {% if column_comment_statements(relation, node.columns, strict=true) %}
      {{ exceptions.raise_compiler_error('Unexpected comment writes for ' ~ name) }}
    {% endif %}
  {% endfor %}
{% endmacro %}
""")

            def dbt(*args):
                result = subprocess.run(
                    [sys.executable, "-m", "dbt.cli.main", *args, "--project-dir", directory,
                     "--profiles-dir", directory, "--no-partial-parse"],
                    capture_output=True, text=True, timeout=120,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

            cursor = connect(host="localhost", port=int(os.environ["COLUMN_COMMENTS_TRINO_PORT"]),
                             user="comment_test", catalog="memory").cursor()
            try:
                dbt("run")
                dbt("run")
                for name in ("fixture_view", "fixture_incremental"):
                    actual = dict(cursor.execute(
                        "select column_name, comment from memory.information_schema.columns "
                        f"where table_schema = '{schema}' and table_name = '{name}'").fetchall())
                    self.assertEqual(actual, {"id": "Identifier", "from": description,
                                              "owner": "Quoted owner", "amount": None})
                rows = cursor.execute(f'select * from memory."{schema}".fixture_incremental').fetchall()
                self.assertEqual(rows, [[7, "abc", 11, 3], [7, "abc", 11, 3]])
                dbt("run-operation", "check_unchanged")
                cursor.execute(f'comment on column memory."{schema}".fixture_view."from" is \'stale\'')
                dbt("run-operation", "backfill_column_comments", "--args",
                    '{unique_ids: [model.comment_test.fixture_view], dry_run: false}')
                dbt("run-operation", "check_unchanged")
            finally:
                for name, kind in (("fixture_view", "view"), ("fixture_incremental", "table")):
                    cursor.execute(f'drop {kind} if exists memory."{schema}".{name}')
                cursor.execute(f'drop schema if exists memory."{schema}"')
