"""Exercise the native persistence and manual operation through dbt's macro runtime."""

import re
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dbt.adapters.sql.connections import SQLConnectionManager
from dbt.adapters.trino.connections import TrinoConnectionManager
from dbt.clients.jinja import MacroGenerator, MacroStack
from dbt.contracts.graph.nodes import Macro
from dbt.node_types import NodeType
from dbt_common.exceptions import CompilationError, MacroReturn


MACRO_DIR = Path(__file__).resolve().parents[1] / "dune"


class Relation(SimpleNamespace):
    def __str__(self):
        return f'"{self.database}"."{self.schema}"."{self.identifier}"'


class ColumnCommentsTest(unittest.TestCase):
    def setUp(self):
        self.writes = []
        self.reads = []
        self.current = [("from", "old sender"), ("Owner", None), ("amount", "stale")]
        self.relation = Relation(database="hive", schema="demo", identifier="amounts")
        self.node = {
            "resource_type": "model", "relation_name": str(self.relation),
            "database": "hive", "schema": "demo", "alias": "amounts",
            "config": {"materialized": "view"}, "description": "",
            "columns": {
                "FROM": {"description": "Sender's project ID -- path \\ snow ☃;\nnext"},
                "Owner": {"quote": True, "description": "Owner"},
                "amount": {"description": ""},
            },
        }

        def return_value(value):
            raise MacroReturn(value)

        def fail(message):
            raise CompilationError(message)

        def run_query(sql):
            if sql.lstrip().startswith("select"):
                self.reads.append(sql)
                return SimpleNamespace(rows=self.current)
            self.writes.append(sql)

        self.ctx = {
            "execute": True, "return": return_value,
            "exceptions": SimpleNamespace(raise_compiler_error=fail),
            "graph": SimpleNamespace(nodes={"model.demo.amounts": self.node}),
            "config": SimpleNamespace(persist_relation_docs=lambda: False,
                                      persist_column_docs=lambda: True),
            "adapter": SimpleNamespace(
                quote=lambda name: '"' + name.replace('"', '""') + '"',
                get_relation=lambda **kwargs: self.relation),
            "run_query": run_query, "log": lambda *args, **kwargs: None,
        }
        stack = MacroStack()
        for filename in ("column_comments.sql", "backfill_column_comments.sql"):
            source = (MACRO_DIR / filename).read_text()
            for match in re.finditer(r"{% macro (\w+).*?{% endmacro %}", source, re.S):
                name = match[1]
                macro = Macro(
                    name=name, resource_type=NodeType.Macro, package_name="test",
                    path=filename, original_file_path=filename,
                    unique_id=f"macro.test.{name}", macro_sql=match[0],
                )
                self.ctx[name] = MacroGenerator(macro, self.ctx, stack=stack)

    def persist(self):
        self.ctx["trino__persist_docs"](self.relation, self.node)

    def backfill(self, ids=None, **kwargs):
        return self.ctx["backfill_column_comments"](
            ["model.demo.amounts"] if ids is None else ids, **kwargs)

    def test_native_persistence_quotes_matches_case_and_preserves_literals(self):
        self.persist()
        expected = (
            'comment on column "hive"."demo"."amounts"."from" is '
            "U&'Sender''s project ID \\002D\\002D path \\005C snow ☃;\\000Anext';\n"
            'comment on column "hive"."demo"."amounts"."Owner" is U&\'Owner\';\n'
            'comment on column "hive"."demo"."amounts"."amount" is null'
        )
        self.assertEqual(self.writes, [expected])
        # Run the real dbt-trino splitter/filter, not a reconstruction of its regex.
        manager = object.__new__(TrinoConnectionManager)
        with patch.object(SQLConnectionManager, "add_query", return_value=(object(), object())) as sent:
            manager.add_query(expected)
        self.assertEqual(len(sent.call_args_list), 3)
        self.assertIn("\\002D\\002D", sent.call_args_list[0].args[0])

    def test_steady_state_and_unmatched_columns_do_not_write(self):
        self.current = [("from", self.node["columns"]["FROM"]["description"]),
                        ("Owner", "Owner"), ("amount", None)]
        self.persist()
        self.assertEqual(self.writes, [])
        self.current = [("undocumented", "keep this comment")]
        self.persist()
        self.assertEqual(self.writes, [])

    def test_only_changed_comments_write_using_physical_column_names(self):
        self.current = [("from", self.node["columns"]["FROM"]["description"]),
                        ("owner", "old owner"), ("amount", "stale")]
        self.persist()
        self.assertEqual(self.writes, [
            'comment on column "hive"."demo"."amounts"."owner" is U&\'Owner\';\n'
            'comment on column "hive"."demo"."amounts"."amount" is null',
        ])

    def test_preview_is_offline_and_apply_uses_the_same_writer(self):
        preview = self.backfill()
        self.assertEqual(self.reads, [])
        self.assertEqual(self.writes, [])
        applied = self.backfill(dry_run=False)
        self.assertEqual(preview, applied)
        self.assertEqual(self.writes, applied)
        self.writes.clear()
        self.current = [("from", self.node["columns"]["FROM"]["description"]),
                        ("Owner", "Owner"), ("amount", None)]
        self.assertEqual(self.backfill(dry_run=False), [])
        self.assertEqual(self.writes, [])

    def test_invalid_ids_and_sources_never_write(self):
        for ids in ([], "model.demo.amounts", ["source.demo.raw.amounts"],
                    ["model.demo.amounts", "model.demo.missing"]):
            with self.subTest(ids=ids), self.assertRaises(CompilationError):
                self.backfill(ids, dry_run=False)
            self.assertEqual(self.writes, [])

    def test_all_physical_columns_are_checked_before_first_write(self):
        missing = {**self.node, "columns": {"missing": {"description": "absent"}}}
        self.ctx["graph"].nodes["model.demo.missing"] = missing
        with self.assertRaisesRegex(CompilationError, "Documented column not found"):
            self.backfill(["model.demo.amounts", "model.demo.missing"], dry_run=False)
        self.assertEqual(self.writes, [])

    def test_missing_relations_and_empty_documentation_skip_writes(self):
        self.ctx["adapter"].get_relation = lambda **kwargs: None
        self.assertEqual(self.backfill(dry_run=False), [])
        self.assertEqual(self.reads, [])
        self.node["columns"] = {}
        self.persist()
        self.assertEqual(self.writes, [])


if __name__ == "__main__":
    unittest.main()
