-- venue-settled 1inch-LOP fills must NOT appear in oneinch_lop_own_trades (dex.trades side):
-- the underlying venue's own row already represents the trade there, and these fills are
-- reclassified into dex_aggregator.trades via oneinch_lop_aggregator_trades.
-- The txs below were live-verified as venue-settled (CUR2-2693). The test goes vacuous once
-- the upstream incremental windows age past these block dates - that's expected; it guards
-- the anti-join wiring at (and around) merge time, not the classification forever.

select blockchain, tx_hash, evt_index
from {{ ref('oneinch_lop_own_trades') }}
where (blockchain, tx_hash) in (
    ('arbitrum', 0x92c5f2295c67cb1def44ba86667a02c6cd4d96b64825dde9fc407d987ddb07cb), -- settled on dodo v2
    ('arbitrum', 0xafefb0e48142bcce632cd5c67a55ca18dadc14390f9d01315cee75472ada376a), -- settled on uniswap
    ('ethereum', 0x959f82dde57fea7d94eb091fa57919cae6fb67a92cf1d4c4b27eceb1a1be4a10)  -- settled on a tracked venue, $1.85M fill
)
and block_date between date '2026-05-20' and date '2026-06-06'
