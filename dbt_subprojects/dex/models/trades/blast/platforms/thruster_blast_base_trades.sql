{{
    config(
        schema = 'thruster_blast',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH

dexs_v1_30bps AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'blast',
            project = 'thruster',
            version = 'UNI-V2-30bps',
            Pair_evt_Swap = source('thruster_blast', 'ThrusterPair_V2_Point_3_Fee_evt_Swap'),
            Factory_evt_PairCreated = source('thruster_blast', 'ThrusterFactory_Point_3_Fee_evt_PairCreated')
        )
    }}
),

dexs_v1_100bps AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'blast',
            project = 'thruster',
            version = 'UNI-V2-100bps',
            Pair_evt_Swap = source('thruster_blast', 'ThrusterPair_V2_1_Fee_evt_Swap'),
            Factory_evt_PairCreated = source('thruster_blast', 'ThrusterFactory_1_Fee_evt_PairCreated')
        )
    }}
),

dexs_v1_univ3 AS (
    {{
        uniswap_compatible_v3_trades(
            blockchain = 'blast'
            , project = 'thruster'
            , version = 'UNI-V3'
            , Pair_evt_Swap = source('thruster_blast', 'ThrusterPool_V3_Point_3_Fee_evt_Swap')
            , Factory_evt_PoolCreated = source('thruster_blast', 'ThrusterPoolFactory_evt_PoolCreated')
        )
    }}
)

SELECT
    dexs_v1_30bps.blockchain,
    dexs_v1_30bps.project,
    dexs_v1_30bps.version,
    dexs_v1_30bps.block_month,
    dexs_v1_30bps.block_date,
    dexs_v1_30bps.block_time,
    dexs_v1_30bps.block_number,
    dexs_v1_30bps.token_bought_amount_raw,
    dexs_v1_30bps.token_sold_amount_raw,
    dexs_v1_30bps.token_bought_address,
    dexs_v1_30bps.token_sold_address,
    dexs_v1_30bps.taker,
    dexs_v1_30bps.maker,
    dexs_v1_30bps.project_contract_address,
    dexs_v1_30bps.tx_hash,
    dexs_v1_30bps.evt_index
FROM dexs_v1_30bps
UNION ALL
SELECT
    dexs_v1_100bps.blockchain,
    dexs_v1_100bps.project,
    dexs_v1_100bps.version,
    dexs_v1_100bps.block_month,
    dexs_v1_100bps.block_date,
    dexs_v1_100bps.block_time,
    dexs_v1_100bps.block_number,
    dexs_v1_100bps.token_bought_amount_raw,
    dexs_v1_100bps.token_sold_amount_raw,
    dexs_v1_100bps.token_bought_address,
    dexs_v1_100bps.token_sold_address,
    dexs_v1_100bps.taker,
    dexs_v1_100bps.maker,
    dexs_v1_100bps.project_contract_address,
    dexs_v1_100bps.tx_hash,
    dexs_v1_100bps.evt_index
FROM dexs_v1_100bps
UNION ALL
SELECT
    dexs_v1_univ3.blockchain,
    dexs_v1_univ3.project,
    dexs_v1_univ3.version,
    dexs_v1_univ3.block_month,
    dexs_v1_univ3.block_date,
    dexs_v1_univ3.block_time,
    dexs_v1_univ3.block_number,
    dexs_v1_univ3.token_bought_amount_raw,
    dexs_v1_univ3.token_sold_amount_raw,
    dexs_v1_univ3.token_bought_address,
    dexs_v1_univ3.token_sold_address,
    dexs_v1_univ3.taker,
    dexs_v1_univ3.maker,
    dexs_v1_univ3.project_contract_address,
    dexs_v1_univ3.tx_hash,
    dexs_v1_univ3.evt_index
FROM dexs_v1_univ3