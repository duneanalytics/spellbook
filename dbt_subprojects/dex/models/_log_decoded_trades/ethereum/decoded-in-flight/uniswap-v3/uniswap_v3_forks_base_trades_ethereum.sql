{{ config(

        schema = 'dex_mass_decoding_ethereum',
        alias = 'uniswap_v3_base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index', 'token_bought_address', 'token_sold_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
        )
}}

WITH all_decoded_trades AS (
    {{
        uniswap_v3_forks_trades(
            blockchain = 'ethereum'
            , version = 'null'
            , project = 'null'
            , Pair_evt_Swap = ref('uniswap_v3_pool_decoding_ethereum')
            , Factory_evt_PoolCreated = ref('uniswap_v3_factory_decoding_ethereum')
        )
    }}
)

SELECT
    fork_mapping.project_name as project
    , all_decoded_trades.*
FROM all_decoded_trades
INNER JOIN {{ ref('uniswap_v3_fork_mapping_ethereum') }} AS fork_mapping
USING (factory_address)