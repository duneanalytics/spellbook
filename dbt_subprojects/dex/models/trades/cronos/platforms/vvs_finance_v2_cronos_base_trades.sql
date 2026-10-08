{{ config(
    schema = 'vvs_finance_v2_cronos'
    , alias = 'base_trades'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

-- Cronos validators rolled the chain back on 2026-08-30 (Tectonic exploit) and discarded blocks
-- 90896189..90907150. Dune's raw cronos tables still carry that dead fork, and the same transactions
-- are now being re-included on the canonical chain with identical tx_hash/evt_index, so the merge
-- key double-matches (MERGE_TARGET_ROW_MULTIPLE_MATCHES, 2026-09-01/02). Frozen fence: exclude the
-- dead-fork block range from both swap and factory sources until raw cronos is re-ingested from
-- the canonical chain, then drop these CTEs and full-refresh the cronos dex models.
-- Pair 0x4dfd091570af4f6b3da9280c2aa7487e61c7c24e was created on the discarded fork (90897058)
-- and re-included canonically (94069876, 2026-09-15 13:41 UTC); the factory join is 1:n on pair
-- address, so the factory fence is what stops the duplicate merge keys.
with pair_swaps as (
    select *
    from {{ source('vvsfinance_cronos', 'VVSPair_evt_Swap') }}
    where not (evt_block_number between 90896189 and 90907150)
    {% if is_incremental() -%}
    and {{ incremental_predicate('evt_block_time') }}
    {% endif -%}
)
, factory_pairs as (
    select *
    from {{ source('vvsfinance_cronos', 'VVSFactory_evt_PairCreated') }}
    where not (evt_block_number between 90896189 and 90907150)
)
, base as (
{{
    uniswap_compatible_v2_trades(
        blockchain = 'cronos'
        , project = 'vvs_finance'
        , version = '2'
        , Pair_evt_Swap = 'pair_swaps'
        , Factory_evt_PairCreated = 'factory_pairs'
    )
}}
)

select *
from base
