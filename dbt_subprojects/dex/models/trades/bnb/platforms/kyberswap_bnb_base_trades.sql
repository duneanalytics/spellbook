{{
    config(
        schema = 'kyberswap_bnb',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{%
    set config_sources = [
        {
            'version': 'elastic_2',
            'source_evt_swap': 'ElasticPoolV2_evt_Swap',
            'source_evt_factory': 'ElasticFactoryV2_evt_PoolCreated'
        },
    ]
%}

WITH 

dexs_classic AS (
    {{
        uniswap_compatible_v2_trades(
            blockchain = 'bnb',
            project = 'kyberswap',
            version = 'classic',
            Pair_evt_Swap = source('kyber_bnb', 'DMMPool_evt_Swap'),
            Factory_evt_PairCreated = source('kyber_bnb', 'DMMFactory_evt_PoolCreated'),
            pair_column_name = 'pool'
        )
    }}
),

dexs_elastic AS (
    {{
        kyberswap_compatible_trades(
            blockchain = 'bnb',
            project = 'kyberswap',
            sources = config_sources
        )
    }}
)

SELECT
    dexs_classic.blockchain,
    dexs_classic.project,
    dexs_classic.version,
    dexs_classic.block_month,
    dexs_classic.block_date,
    dexs_classic.block_time,
    dexs_classic.block_number,
    dexs_classic.token_bought_amount_raw,
    dexs_classic.token_sold_amount_raw,
    dexs_classic.token_bought_address,
    dexs_classic.token_sold_address,
    dexs_classic.taker,
    dexs_classic.maker,
    dexs_classic.project_contract_address,
    dexs_classic.tx_hash,
    dexs_classic.evt_index
FROM dexs_classic
UNION ALL
SELECT
    dexs_elastic.blockchain,
    dexs_elastic.project,
    dexs_elastic.version,
    dexs_elastic.block_month,
    dexs_elastic.block_date,
    dexs_elastic.block_time,
    dexs_elastic.block_number,
    dexs_elastic.token_bought_amount_raw,
    dexs_elastic.token_sold_amount_raw,
    dexs_elastic.token_bought_address,
    dexs_elastic.token_sold_address,
    dexs_elastic.taker,
    dexs_elastic.maker,
    dexs_elastic.project_contract_address,
    dexs_elastic.tx_hash,
    dexs_elastic.evt_index
FROM dexs_elastic
