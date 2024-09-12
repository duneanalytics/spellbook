{{ config(

        schema = 'dex_mass_decoding_arbitrum',
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
            blockchain = 'arbitrum'
            , version = 'null'
            , project = 'null'
            , Pair_evt_Swap = ref('uniswap_v3_pool_decoding_arbitrum')
            , Factory_evt_PoolCreated = ref('uniswap_v3_factory_decoding_arbitrum')
        )
    }}
)

SELECT
    all_decoded_trades.blockchain
    , fork_mapping.project_name as project
    , all_decoded_trades.version
    , all_decoded_trades.dex_type
    , factory_address
    , all_decoded_trades.block_month
    , all_decoded_trades.block_date
    , all_decoded_trades.block_time
    , all_decoded_trades.block_number
    , all_decoded_trades.token_bought_amount_raw
    , all_decoded_trades.token_sold_amount_raw
    , all_decoded_trades.token_bought_address
    , all_decoded_trades.token_sold_address
    , all_decoded_trades.taker
    , all_decoded_trades.maker
    , all_decoded_trades.project_contract_address
    , all_decoded_trades.tx_hash
    , all_decoded_trades.evt_index
FROM all_decoded_trades
INNER JOIN {{ ref('uniswap_v3_fork_mapping_arbitrum') }} AS fork_mapping
USING (factory_address)