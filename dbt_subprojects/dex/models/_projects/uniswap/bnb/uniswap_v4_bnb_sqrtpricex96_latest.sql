{{ config(
    schema = 'uniswap_v4_bnb'
    , alias = 'sqrtpricex96_latest'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['id', 'blockchain']
    )
}}

-- Forward-filled "latest settled price per pool" companion for uniswap_v4_bnb_sqrtpricex96. Seeds that
-- builder's incremental prior-price lookup so it no longer re-scans full history every run. CUR2-2835.
--
-- BACKFILL NOTE (one-time, prod): before the first scheduled build, pre-seed this table from the
-- existing builder output to skip a full-history transactions join:
--   CREATE TABLE uniswap_v4_bnb.sqrtpricex96_latest AS
--   SELECT blockchain, id,
--          max_by(block_time, block_index_sum)    AS block_time,
--          max_by(block_number, block_index_sum)  AS block_number,
--          max_by(evt_index, block_index_sum)     AS evt_index,
--          max(block_index_sum)                   AS block_index_sum,
--          max_by(sqrtpricex96, block_index_sum)  AS sqrtpricex96
--   FROM uniswap_v4_bnb.sqrtpricex96 GROUP BY blockchain, id;

{{
    uniswap_compatible_v4_liquidity_sqrtpricex96_latest(
          blockchain = 'bnb'
        , project = 'uniswap'
        , version = '4'
        , PoolManager_evt_Initialize = source('uniswap_v4_bnb', 'PoolManager_evt_Initialize')
        , PoolManager_evt_Swap = source('uniswap_v4_bnb', 'PoolManager_evt_Swap')
        , transactions = source('bnb', 'transactions')
    )
}}
