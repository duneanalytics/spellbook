"""Exercise the production ERC4626 event filter without warehouse access.

Run: uv run python -m unittest discover -s scripts/tests -p test_erc4626_async_withdrawals.py
"""
import re
import sqlite3
import unittest
from pathlib import Path

from jinja2 import Environment, StrictUndefined


VAULT = 'cda323c2df692d989b24ba51d0acca924cf9a344'
OTHER_VAULT = '1111111111111111111111111111111111111111'


class AsyncWithdrawalsTest(unittest.TestCase):
    def raw_events(self, chain, incremental=False):
        macro = Path(__file__).resolve().parents[2] / (
            'dbt_subprojects/tokens/macros/transfers/transfers_base_erc4626.sql'
        )
        env = Environment(undefined=StrictUndefined)
        env.globals.update(
            source=lambda schema, name: 'tokens_erc20',
            is_incremental=lambda: incremental,
            incremental_predicate=lambda column: column + " >= '2026-11-01'",
            dbt_utils={'generate_surrogate_key': lambda columns: "'test-key'"},
        )
        sql = env.from_string(macro.read_text()).module.transfers_base_erc4626(
            chain, 'transactions', 'transfers', 'deposits', 'withdrawals'
        )
        # Execute the actual first CTE. Adapt only binary literals and typed NULLs
        # to SQLite; preserve the production filters and both UNION branches.
        sql = sql.split('\n, erc4626_synthetic as (', 1)[0]
        sql = sql.replace('cast(null as array<bigint>)', 'null')
        sql = re.sub(r'0x([0-9a-f]+)', r"'\1'", sql)
        sql += '\nselect contract_address, direction, amount_raw from erc4626_synthetic_raw'

        with sqlite3.connect(':memory:') as db:
            db.execute('create table tokens_erc20 (contract_address text, blockchain text)')
            for table in ('deposits', 'withdrawals'):
                db.execute(f'''create table {table} (
                    evt_block_date text, evt_block_time text, evt_block_number integer,
                    evt_tx_hash text, evt_index integer, contract_address text,
                    owner text, shares integer)''')
            for address in (VAULT, OTHER_VAULT):
                db.execute('insert into tokens_erc20 values (?, ?)', (address, chain))
                db.execute('insert into deposits values (?, ?, ?, ?, ?, ?, ?, ?)',
                           ('2026-11-02', '2026-11-02', 1, 'deposit', 1, address, 'owner', 300000))
                db.execute('insert into withdrawals values (?, ?, ?, ?, ?, ?, ?, ?)',
                           ('2026-11-02', '2026-11-02', 2, 'claim', 1082, address, 'owner', 300000))
            db.execute('insert into withdrawals values (?, ?, ?, ?, ?, ?, ?, ?)',
                       ('2026-10-01', '2026-10-01', 0, 'old-claim', 1, OTHER_VAULT, 'owner', 100))
            return db.execute(sql).fetchall()

    def test_ethereum_excludes_only_kamui_withdrawals(self):
        for incremental in (False, True):
            with self.subTest(incremental=incremental):
                rows = self.raw_events('ethereum', incremental)
                self.assertNotIn((VAULT, 'burn', 300000), rows)
                self.assertIn((VAULT, 'mint', 300000), rows)
                self.assertIn((OTHER_VAULT, 'burn', 300000), rows)
                self.assertEqual((OTHER_VAULT, 'burn', 100) in rows, not incremental)

    def test_same_address_on_other_chains_is_unchanged(self):
        for chain in ('base', 'bnb'):
            for incremental in (False, True):
                with self.subTest(chain=chain, incremental=incremental):
                    self.assertIn((VAULT, 'burn', 300000), self.raw_events(chain, incremental))


if __name__ == '__main__':
    unittest.main()
