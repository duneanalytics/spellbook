"""Test decoded-name normalization and overlap precedence without warehouse access.

Run: uv run python -m unittest discover -s scripts/tests -p test_orca_whirlpool_decoded_calls.py
"""
import sqlite3
import unittest
from pathlib import Path

from jinja2 import Environment, StrictUndefined


class DecodedCallsTest(unittest.TestCase):
    def render(self, bounded=False):
        root = Path(__file__).resolve().parents[2]
        macro = root / 'dbt_subprojects/solana/macros/_sector/dex/orca_whirlpool_decoded_calls.sql'
        env = Environment(undefined=StrictUndefined)
        env.globals.update(
            source=lambda schema, name: name,
            is_incremental=lambda: False,
            incremental_predicate=lambda column: column + " >= '2026-09-09'",
        )
        module = env.from_string(macro.read_text()).module
        return module.orca_whirlpool_decoded_calls(
            'legacy', 'current', [('old_value', 'new_value')], bounded=bounded
        )

    def test_overlap_prefers_current_and_preserves_legacy_history(self):
        db = sqlite3.connect(':memory:')
        metadata = '''call_block_time text, call_block_date text, call_block_slot integer,
            call_tx_id text, call_tx_index integer, call_outer_instruction_index integer,
            call_inner_instruction_index integer, call_is_inner integer,
            call_tx_signer text, call_outer_executing_account text'''
        db.execute('create table legacy (' + metadata + ', old_value text)')
        db.execute('create table current (' + metadata + ', new_value text)')
        def add(table, tx, inner, value):
            db.execute('insert into ' + table + ' values (?,?,?,?,?,?,?,?,?,?,?)',
                       ('2026-09-09', '2026-09-09', 10, tx, 1, 2, inner, 0, 'signer', 'orca', value))
        add('legacy', 'overlap', None, 'old')
        add('current', 'overlap', 0, 'new')
        add('current', 'overlap', 3, 'other_instruction')
        add('legacy', 'historical', None, 'retained')
        add('current', 'recent', None, 'added')
        rows = db.execute(self.render()).fetchall()
        self.assertEqual(len(rows), 4)
        self.assertEqual({row[-1] for row in rows}, {'new', 'other_instruction', 'retained', 'added'})
        db.close()

    def test_only_time_series_calls_are_bounded(self):
        self.assertNotIn("call_block_time >= '2026-09-09'", self.render())
        self.assertEqual(self.render(bounded=True).count("call_block_time >= '2026-09-09'"), 2)


if __name__ == '__main__':
    unittest.main()
