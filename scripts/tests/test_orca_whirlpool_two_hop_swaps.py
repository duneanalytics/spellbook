"""Exercise the rendered matcher with synthetic transfer/instruction fixtures."""
import sqlite3
import unittest
from pathlib import Path

from jinja2 import Environment, StrictUndefined


WHIRLPOOL = 'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'


class TwoHopMatchingTest(unittest.TestCase):
    def run_matcher(self, nested=False, duplicate=False, outside=False, next_program=WHIRLPOOL):
        db = sqlite3.connect(':memory:')
        fields = ['call_tx_id', 'call_block_slot', 'call_tx_index', 'call_block_date',
                  'call_block_time', 'call_outer_instruction_index', 'call_inner_instruction_index',
                  'call_is_inner', 'call_tx_signer', 'call_outer_executing_account',
                  'account_whirlpoolOne', 'account_whirlpoolTwo', 'account_tokenMintInput',
                  'account_tokenMintIntermediate', 'account_tokenMintOutput',
                  'account_tokenOwnerAccountInput', 'account_tokenOwnerAccountOutput',
                  'account_tokenVaultOneInput', 'account_tokenVaultOneIntermediate',
                  'account_tokenVaultTwoIntermediate', 'account_tokenVaultTwoOutput']
        db.execute('create table calls_fixture (' + ','.join(fields) + ')')
        start = 4 if nested else 0
        values = ['tx', 100, 1, '2026-09-09', '2026-09-09', 2, start if nested else None,
                  int(nested), 'user', 'router', 'pool1', 'pool2', 'A', 'B', 'C',
                  'userA', 'userC', 'pool1A', 'pool1B', 'pool2B', 'pool2C']
        db.execute('insert into calls_fixture values (' + ','.join('?' for _ in fields) + ')', values)
        db.execute('create table instruction_calls (tx_id,block_slot,outer_instruction_index,inner_instruction_index,is_inner,executing_account,executing_account_prefix,block_time)')
        db.execute('insert into instruction_calls values (?,?,?,?,?,?,?,?)',
                   ('tx', 100, 2, start if nested else None, int(nested), WHIRLPOOL, 'wh', '2026-09-09'))
        if nested:
            # The next whirlpool instruction in the same outer instruction closes the frame.
            db.execute('insert into instruction_calls values (?,?,?,?,?,?,?,?)',
                       ('tx', 100, 2, 20, 1, next_program, next_program[:2], '2026-09-09'))
        db.execute('create table transfers (tx_id,block_slot,block_date,outer_instruction_index,inner_instruction_index,token_mint_address,from_token_account,to_token_account)')
        # Gaps model memo/hook instructions: matching must not assume consecutive offsets.
        transfers = [(start+2, 'A', 'userA', 'pool1A'),
                     (start+5, 'B', 'pool1B', 'pool2B'),
                     (21 if outside else start+8, 'C', 'pool2C', 'userC'),
                     (start+3, 'B', 'unrelated', 'pool2B')]
        if duplicate:
            transfers.append((start+6, 'B', 'pool1B', 'pool2B'))
        for transfer in transfers:
            db.execute('insert into transfers values (?,?,?,?,?,?,?,?)',
                       ('tx',100,'2026-09-09',2,*transfer))
        env = Environment(undefined=StrictUndefined)
        env.globals.update(
            source=lambda schema, name: 'instruction_calls', ref=lambda name: 'transfers',
            is_incremental=lambda: True, incremental_predicate=lambda column: '1=1',
            orca_whirlpool_decoded_calls=lambda *args, **kwargs: 'select * from calls_fixture',
        )
        root = Path(__file__).resolve().parents[2]
        path = root / 'dbt_subprojects/solana/macros/_sector/dex/orca_whirlpool_two_hop_swaps.sql'
        sql = env.from_string(path.read_text()).module.orca_whirlpool_two_hop_swaps()
        sql = sql.replace('count_if(', 'sum(').replace(
            'cross join unnest(array[1, 2]) as hops(hop)',
            'cross join (select 1 as hop union all select 2 as hop) hops')
        rows = db.execute(sql).fetchall()
        db.close()
        return rows

    def test_direct_two_legs_share_middle_transfer(self):
        rows = self.run_matcher()
        self.assertEqual(len(rows), 2)
        legs = sorted((r[0], r[-2], r[-1], r[2]) for r in rows)
        self.assertEqual(legs, [('pool1', 2, 5, 0), ('pool2', 5, 8, 2)])

    def test_nested_memos_and_unrelated_vaults(self):
        rows = self.run_matcher(nested=True)
        self.assertEqual(sorted((r[-2], r[-1]) for r in rows), [(6, 9), (9, 12)])

    def test_ambiguous_transfers_fail_closed(self):
        self.assertEqual(self.run_matcher(duplicate=True), [])

    def test_transfer_outside_call_frame_is_rejected(self):
        self.assertEqual(self.run_matcher(nested=True, outside=True), [])

    def test_frame_ignores_other_programs(self):
        # A following non-whirlpool instruction does not close the frame, so the
        # output transfer after it is still attributed to this call.
        rows = self.run_matcher(nested=True, outside=True, next_program='RaydiumProgram')
        self.assertEqual(sorted((r[-2], r[-1]) for r in rows), [(6, 9), (9, 21)])

    def test_frame_scan_is_restricted_to_whirlpool_program(self):
        env = Environment(undefined=StrictUndefined)
        env.globals.update(
            source=lambda schema, name: name, ref=lambda name: name,
            is_incremental=lambda: False, incremental_predicate=lambda column: '1=1',
            orca_whirlpool_decoded_calls=lambda *args, **kwargs: 'calls_fixture',
        )
        root = Path(__file__).resolve().parents[2]
        path = root / 'dbt_subprojects/solana/macros/_sector/dex/orca_whirlpool_two_hop_swaps.sql'
        sql = env.from_string(path.read_text()).module.orca_whirlpool_two_hop_swaps()
        self.assertIn("i.executing_account_prefix = 'wh'", sql)
        self.assertIn("i.executing_account = '" + WHIRLPOOL + "'", sql)


if __name__ == '__main__':
    unittest.main()
