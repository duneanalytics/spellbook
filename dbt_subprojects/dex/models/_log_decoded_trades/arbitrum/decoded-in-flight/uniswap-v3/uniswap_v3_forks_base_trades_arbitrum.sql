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
            , version = '3'
            , project = 'null'
            , Pair_evt_Swap = ref('uniswap_v3_pool_decoding_arbitrum')
            , Factory_evt_PoolCreated = ref('uniswap_v3_factory_decoding_arbitrum')
        )
    }}
)

SELECT  uniswap_v3_base_trades.blockchain
        , contracts.namespace AS project
        , uniswap_v3_base_trades.version
        , uniswap_v3_base_trades.dex_type
        , uniswap_v3_base_trades.factory_address
        , uniswap_v3_base_trades.block_month
        , uniswap_v3_base_trades.block_date
        , uniswap_v3_base_trades.block_time
        , uniswap_v3_base_trades.block_number
        , uniswap_v3_base_trades.token_bought_amount_raw
        , uniswap_v3_base_trades.token_sold_amount_raw
        , uniswap_v3_base_trades.token_bought_address
        , uniswap_v3_base_trades.token_sold_address
        , uniswap_v3_base_trades.taker
        , uniswap_v3_base_trades.maker
        , uniswap_v3_base_trades.project_contract_address
        , uniswap_v3_base_trades.tx_hash
        , uniswap_v3_base_trades.evt_index
FROM all_decoded_trades AS uniswap_v3_base_trades
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
ON transfers.tx_hash = uniswap_v3_base_trades.tx_hash
    AND contains(transfers.contract_addresses, uniswap_v3_base_trades.token_bought_address)
    AND contains(transfers.contract_addresses, uniswap_v3_base_trades.token_sold_address)
LEFT JOIN {{ source('evms', 'contracts') }} AS contracts
ON uniswap_v3_base_trades.project_contract_address = contracts.address
  AND uniswap_v3_base_trades.blockchain = contracts.blockchain