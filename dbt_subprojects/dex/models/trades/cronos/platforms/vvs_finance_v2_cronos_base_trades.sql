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

with base as (
{{
    uniswap_compatible_v2_trades(
        blockchain = 'cronos'
        , project = 'vvs_finance'
        , version = '2'
        , Pair_evt_Swap = source('vvsfinance_cronos', 'VVSPair_evt_Swap')
        , Factory_evt_PairCreated = source('vvsfinance_cronos', 'VVSFactory_evt_PairCreated')
    )
}}
)

select *
from base
-- Cronos validators rolled the chain back on 2026-08-30 (Tectonic exploit) and discarded blocks
-- 90896189..90907150. Dune's raw cronos tables still carry that dead fork, and the same transactions
-- are now being re-included on the canonical chain with identical tx_hash/evt_index, so the merge
-- key double-matches (MERGE_TARGET_ROW_MULTIPLE_MATCHES, 2026-09-01/02). Frozen fence: exclude the
-- dead-fork block range until raw cronos is re-ingested from the canonical chain, then drop this
-- filter and full-refresh the cronos dex models.
where not (block_number between 90896189 and 90907150)
