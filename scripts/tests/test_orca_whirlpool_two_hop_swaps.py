"""Exercise the rendered matcher with synthetic transfer/instruction fixtures."""
import sqlite3
import unittest
from pathlib import Path

from jinja2 import Environment, StrictUndefined


class TwoHopMatchingTest(unittest.TestCase):
    def run_matcher(self, nested=False, duplicate=False, outside=False):
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
        db.execute('create table instruction_calls (tx_id,block_slot,outer_instruction_index,inner_instruction_index,stack_height,block_time)')
        db.execute('insert into instruction_calls values (?,?,?,?,?,?)',
                   ('tx', 100, 2, start if nested else None, 2 if nested else 1, '2026-09-09'))
        if nested:
            db.execute("insert into instruction_calls values ('tx',100,2,20,2,'2026-09-09')")
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
        self.assertEqual([(r[0], r[-2], r[-1]) for r in rows], [('pool1', 2, 5), ('pool2', 5, 8)])
        self.assertEqual([r[2] for r in rows], [0, 2])

    def test_nested_memos_and_unrelated_vaults(self):
        rows = self.run_matcher(nested=True)
        self.assertEqual([(r[-2], r[-1]) for r in rows], [(6, 9), (9, 12)])

    def test_ambiguous_transfers_fail_closed(self):
        self.assertEqual(self.run_matcher(duplicate=True), [])

    def test_transfer_outside_call_frame_is_rejected(self):
        self.assertEqual(self.run_matcher(nested=True, outside=True), [])


if __name__ == '__main__':
    unittest.main()
