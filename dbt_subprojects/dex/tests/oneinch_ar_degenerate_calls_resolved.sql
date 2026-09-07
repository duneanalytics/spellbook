-- Regression for 1inch AR v6 swaps with synthetic decoded params (srcToken = dstToken = marker
-- address, amounts = 1; e.g. swaps routed by the Binance wallet proxy): the executions macro must
-- recover the real token legs by netting the nested transfers against the venue side (SIM-6142).
-- The anchor tx settles ~101.8 USDT -> ~13.2 Beat through the PancakeSwap Infinity vault, so no
-- decoded field nor any directly matched transfer carries the user-level legs.
-- The test anchors at the executions model because slim CI rebuilds it but cannot materialize the
-- full-history oneinch_swaps that oneinch_ar_trades reads; the trades view is a thin projection of
-- these fields. It goes vacuous once the rebuilt window ages past this block date - that's
-- expected; it guards the netting logic whenever the anchor is visible, not forever.
select blockchain, tx_hash, call_trace_address, src_token_address, dst_token_address, src_executed_amount, dst_executed_amount, src_executed_symbol, dst_executed_symbol
from {{ ref('oneinch_bnb_ar_executions') }}
where block_month = date '2026-06-01'
    and block_date = date '2026-06-12'
    and tx_hash = 0x463b95a76eb82b6fda8e8a51f96291fef9714c550f9de17f14a37544a89ecfa9
    and not coalesce(
        src_token_address = 0x55d398326f99059ff775485246999027b3197955 -- USDT
        and dst_token_address = 0xcf3232b85b43bca90e51d38cc06cc8bb8c8a3e36 -- Beat
        and src_executed_address = 0x55d398326f99059ff775485246999027b3197955
        and dst_executed_address = 0xcf3232b85b43bca90e51d38cc06cc8bb8c8a3e36
        and src_executed_amount = uint256 '101815292112449005466'
        and dst_executed_amount = uint256 '13199978474816822881'
        and src_executed_symbol = 'USDT'
        and dst_executed_symbol = 'Beat'
        and element_at(complement, 'src_decimals') = '18'
        and element_at(complement, 'dst_decimals') = '18'
    , false)
