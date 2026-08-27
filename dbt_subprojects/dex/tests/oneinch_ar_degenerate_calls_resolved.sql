-- E2E regression for 1inch AR v6 swaps with synthetic decoded params (srcToken = dstToken = marker
-- address, amounts = 1; e.g. swaps routed by the Binance wallet proxy): the executions macro must
-- recover the real token legs by netting the nested transfers against the venue side (SIM-6142).
-- The anchor tx settles ~101.8 USDT -> ~13.2 Beat through the PancakeSwap Infinity vault, so no
-- decoded field nor any directly matched transfer carries the user-level legs.
-- The test goes vacuous once the upstream incremental windows age past this block date - that's
-- expected; it guards the netting logic while the rebuilt window covers the anchor, not forever.
select blockchain, tx_hash, token_sold_address, token_bought_address, token_sold_amount_raw, token_bought_amount_raw, token_sold_symbol, token_bought_symbol
from {{ ref('oneinch_ar_trades') }}
where blockchain = 'bnb'
    and block_date = date '2026-06-12'
    and tx_hash = 0x463b95a76eb82b6fda8e8a51f96291fef9714c550f9de17f14a37544a89ecfa9
    and not coalesce(
        token_sold_address = 0x55d398326f99059ff775485246999027b3197955 -- USDT
        and token_bought_address = 0xcf3232b85b43bca90e51d38cc06cc8bb8c8a3e36 -- Beat
        and token_sold_amount_raw = uint256 '101815292112449005466'
        and token_bought_amount_raw = uint256 '13199978474816822881'
        and token_sold_symbol = 'USDT'
        and token_bought_symbol = 'Beat'
        and token_pair = 'Beat-USDT'
    , false)
