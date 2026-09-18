"""Exercise the model's raw V2 selector with accepted and rejected calls."""
import json
from contextlib import closing
from pathlib import Path
import sqlite3
import unittest

from jinja2 import Environment, StrictUndefined
import sqlglot
from sqlglot import exp


MODEL = Path(__file__).resolve().parents[2] / (
    'dbt_subprojects/solana/models/_sector/dex/raydium/staging/'
    'raydium_v4_solana_stg_decoded_swaps.sql'
)
PROGRAM = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8'
TOKEN = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'


class RaydiumV2SelectorTest(unittest.TestCase):
    def select(self, overrides, incremental=False):
        sql = Environment(undefined=StrictUndefined).from_string(MODEL.read_text()).render(
            config=lambda **kwargs: '',
            source=lambda schema, table: table,
            is_incremental=lambda: incremental,
            incremental_predicate=lambda column: f"{column} >= timestamp '2026-09-07'",
            solana_instruction_key=lambda *args: '0',
        )
        tree = sqlglot.parse_one(sql, read='trino')
        selector = next(
            node for node in tree.find_all(exp.Select)
            if isinstance(node.args.get('from_'), exp.From)
            and isinstance(node.args['from_'].this, exp.Table)
            and node.args['from_'].this.name == 'instruction_calls'
        ).copy()
        # SQLite CAST(... AS TIMESTAMP) produces a year integer, so retain ISO text.
        selector = selector.transform(
            lambda node: node.this if isinstance(node, exp.Cast)
            and node.to.is_type(exp.DataType.Type.TIMESTAMP) else node
        )
        row = dict(
            account_arguments=json.dumps([TOKEN, 'pool', 'authority', 'vault_a', 'vault_b', 'user_in', 'user_out', 'signer']),
            is_inner=True, outer_instruction_index=3, inner_instruction_index=6,
            tx_id='tx', block_time='2026-09-07 03:46:51', block_slot=444966532,
            block_date='2026-09-07', outer_executing_account='router', tx_signer='signer',
            tx_index=1103, executing_account=PROGRAM, executing_account_prefix='67',
            tx_success=True, data=bytes([16]) + bytes(16),
        )
        row.update(overrides)
        with closing(sqlite3.connect(':memory:')) as db:
            db.create_function('bytearray_substring', 3, lambda data, start, size: data[start - 1:start - 1 + size])
            db.create_function('cardinality', 1, lambda value: len(json.loads(value)))
            db.create_function('element_at', 2, lambda value, index: (json.loads(value) + [None] * index)[index - 1])
            db.execute('create table instruction_calls (' + ', '.join(row) + ')')
            db.execute('insert into instruction_calls values (' + ','.join('?' for _ in row) + ')', list(row.values()))
            # Keep Dune's scalar function names; only adapt hex literals to SQLite.
            rendered = selector.sql(dialect='trino').replace('0x10', "X'10'").replace('0x11', "X'11'")
            return db.execute(rendered).fetchall()

    def test_both_opcodes_and_direct_or_inner_calls(self):
        for opcode in (16, 17):
            for inner in (False, True):
                with self.subTest(opcode=opcode, inner=inner):
                    rows = self.select(dict(data=bytes([opcode]) + bytes(16), is_inner=inner))
                    self.assertEqual(len(rows), 1)
                    self.assertEqual(rows[0][0], 'pool')
                    self.assertEqual(rows[0][1], inner)

    def test_excludes_failed_legacy_wrong_program_and_invalid_layouts(self):
        for override in (
            {'tx_success': False}, {'executing_account': 'other'},
            {'data': bytes([9]) + bytes(16)}, {'data': bytes([11]) + bytes(16)},
            {'data': bytes([16]) + bytes(15)}, {'data': b''},
            {'account_arguments': json.dumps([TOKEN] * 7)},
            {'account_arguments': json.dumps(['other_token'] * 8)},
        ):
            with self.subTest(override=override):
                self.assertEqual(self.select(override), [])

    def test_historical_and_incremental_bounds(self):
        self.assertEqual(len(self.select({'block_time': '2025-01-01'})), 1)
        self.assertEqual(self.select({'block_time': '2025-01-01'}, incremental=True), [])
        self.assertEqual(len(self.select({}, incremental=True)), 1)
        self.assertEqual(self.select({'block_time': '2020-01-01'}), [])


if __name__ == '__main__':
    unittest.main()
