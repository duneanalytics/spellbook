{{
    config(
        schema = 'syncswap_zksync',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set syncswap_start_date = "2023-03-24" %}

WITH 
    -- All SyncSwap Pools
    pools AS (
        SELECT pool, token0, token1
        FROM {{ source('syncswap_zksync', 'SyncSwapClassicPoolFactory_evt_PoolCreated') }}
        UNION ALL 
        SELECT pool, token0, token1
        FROM {{ source('syncswap_zksync', 'SyncSwapStablePoolFactory_evt_PoolCreated') }}
    )

    , logs AS ( 
        SELECT 
            *
            , bytearray_substring(topic1, 13, 20) AS caller
            , bytearray_substring(topic2, 13, 20) AS swapper
            , varbinary_to_uint256(bytearray_substring(data, 1, 32)) AS token_0_in
            , varbinary_to_uint256(bytearray_substring(data, 33, 32)) AS token_1_in
            , varbinary_to_uint256(bytearray_substring(data, 65, 32)) AS token_0_out
            , varbinary_to_uint256(bytearray_substring(data, 97, 32)) AS token_1_out
        FROM {{ source('zksync', 'logs') }}
        LEFT JOIN pools ON contract_address = pool
        WHERE contract_address IN (SELECT pool FROM pools) 
            AND topic0 = 0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822 -- swap
            {% if is_incremental() %}
            AND {{incremental_predicate('block_time')}}
            {% else %}
            AND block_time >= timestamp '{{syncswap_start_date}}'
            {% endif %}
    )
    
SELECT
    'zksync' AS blockchain
    , 'syncswap' As project
    , '1' AS version
    , CAST(date_trunc('month', block_time) AS DATE) AS block_month
    , CAST(date_trunc('day', block_time) AS DATE) AS block_date
    , block_time AS block_time
    , block_number AS block_number
    , IF(token_0_out = 0, token_1_out, token_0_out) AS token_bought_amount_raw
    , IF(token_0_in = 0, token_1_in, token_0_in) AS token_sold_amount_raw
    , IF(token_0_out = 0, token1, token0) AS token_bought_address
    , IF(token_0_in = 0, token1, token0) AS token_sold_address
    , tx_from AS taker
    , CAST(NULL AS VARBINARY) AS maker
    , contract_address AS project_contract_address
    , tx_hash
    , index AS evt_index
FROM logs
