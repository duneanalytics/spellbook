{{ config(

        schema = 'dex_mass_decoding_arbitrum',
        alias = 'uniswap_v2_base_trades',
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
        uniswap_v2_forks_trades(
            blockchain = 'arbitrum'
            , version = 'null'
            , project = 'null'
            , Pair_evt_Swap = ref('uniswap_v2_pool_decoding_arbitrum')
            , Factory_evt_PairCreated = ref('uniswap_v2_factory_decoding_arbitrum')
        )
    }}
)

SELECT  {% if is_incremental() %}
        DISTINCT
        {% endif %}
        uniswap_v2_base_trades.*
FROM all_decoded_trades AS uniswap_v2_base_trades
INNER JOIN (
    SELECT
        tx_hash,
        array_agg(DISTINCT contract_address) as contract_addresses
    FROM {{ source('tokens', 'transfers') }}
    WHERE blockchain = 'arbitrum'
        {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {% endif %}
    GROUP BY tx_hash
) AS transfers
ON transfers.tx_hash = uniswap_v2_base_trades.tx_hash
    AND contains(transfers.contract_addresses, uniswap_v2_base_trades.token_bought_address)
    AND contains(transfers.contract_addresses, uniswap_v2_base_trades.token_sold_address)