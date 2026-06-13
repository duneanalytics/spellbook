{{
    config(
        schema = 'pharaoh_v3_dlmm_avalanche_c',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set pharaoh_v3_dlmm_start_date = "2026-05-01" %}
{% set pharaoh_v3_dlmm_factory = "0xeb480050b016f6c6d45203d2346b68bddda23d4d" %}
{% set lb_pair_created_topic = "0x2c8d104b27c6b7f4492017a6f5cf3803043688934ebcaa6a03540beeaf976aff" %}
{% set lb_swap_topic = "0xad7d6f97abf51ce18e17a38f4d70e975be9c0708474987bb3e26ad21bd93ca70" %}

WITH pool_created AS (
    SELECT
        bytearray_substring(topic1, 13, 20) AS tokenX
        , bytearray_substring(topic2, 13, 20) AS tokenY
        , bytearray_to_uint256(topic3) AS bin_step
        , bytearray_substring(data, 13, 20) AS lb_pair
    FROM {{ source('avalanche_c', 'logs') }}
    WHERE block_time >= TIMESTAMP '{{ pharaoh_v3_dlmm_start_date }}'
        AND contract_address = {{ pharaoh_v3_dlmm_factory }}
        AND topic0 = {{ lb_pair_created_topic }}
),

swaps AS (
    SELECT
        block_number
        , block_time
        , tx_hash
        , index AS evt_index
        , bytearray_substring(topic2, 13, 20) AS taker
        , contract_address
        , bytearray_substring(data, 33, 32) AS amounts_in
        , bytearray_substring(data, 65, 32) AS amounts_out
    FROM {{ source('avalanche_c', 'logs') }}
    WHERE block_time >= TIMESTAMP '{{ pharaoh_v3_dlmm_start_date }}'
        AND topic0 = {{ lb_swap_topic }}
    {% if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
    {% endif %}
),

decoded_swaps AS (
    SELECT
        s.block_number
        , s.block_time
        , s.tx_hash
        , s.evt_index
        , s.taker
        , s.contract_address
        , p.tokenX
        , p.tokenY
        , CAST(bytearray_to_int256(bytearray_substring(amounts_in, 17, 32)) AS INT256)
            - CAST(bytearray_to_int256(bytearray_substring(amounts_out, 17, 32)) AS INT256) AS amount0
        , CAST(bytearray_to_int256(bytearray_substring(amounts_in, 1, 16)) AS INT256)
            - CAST(bytearray_to_int256(bytearray_substring(amounts_out, 1, 16)) AS INT256) AS amount1
    FROM swaps s
    INNER JOIN pool_created p
        ON p.lb_pair = s.contract_address
)

SELECT
    'avalanche_c' AS blockchain
    , 'pharaoh_v3' AS project
    , 'dlmm' AS version
    , CAST(date_trunc('month', block_time) AS date) AS block_month
    , CAST(date_trunc('day', block_time) AS date) AS block_date
    , block_time
    , block_number
    , ABS(CASE WHEN amount0 < INT256 '0' THEN amount0 ELSE amount1 END) AS token_bought_amount_raw
    , ABS(CASE WHEN amount0 < INT256 '0' THEN amount1 ELSE amount0 END) AS token_sold_amount_raw
    , CASE WHEN amount0 < INT256 '0' THEN tokenX ELSE tokenY END AS token_bought_address
    , CASE WHEN amount0 < INT256 '0' THEN tokenY ELSE tokenX END AS token_sold_address
    , taker
    , contract_address AS maker
    , contract_address AS project_contract_address
    , tx_hash
    , evt_index
FROM decoded_swaps
