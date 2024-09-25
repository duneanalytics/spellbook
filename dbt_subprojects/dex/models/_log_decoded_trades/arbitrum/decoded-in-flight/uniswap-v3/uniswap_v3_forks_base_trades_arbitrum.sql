{{ config(

        schema = 'dex_mass_decoding_arbitrum',
        alias = 'uniswap_v3_base_trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
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

SELECT uniswap_v3_base_trades.*
FROM all_decoded_trades AS uniswap_v3_base_trades
INNER JOIN (
    SELECT
        count(*) as transfer_count,
        token_address,
        evt_tx_hash
    FROM {{ ref('transfers_arbitrum_erc20') }}
    GROUP BY token_address, evt_tx_hash
    HAVING count(*) >= 1
) AS transfers
ON (transfers.evt_tx_hash = uniswap_v3_base_trades.tx_hash)
   AND (transfers.token_address = uniswap_v3_base_trades.token_bought_address
        OR transfers.token_address = uniswap_v3_base_trades.token_sold_address)
