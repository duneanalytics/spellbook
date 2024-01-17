{{ config(
    schema = 'stars_arena_avalanche_c',
    alias = 'base_trades',
    file_format = 'delta',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index']
    )
}}

{% set stars_arena_start_date = '2023-09-20' %}

SELECT 
    'avalanche_c' AS blockchain
    , txs.block_time
    , txs.block_number
    , 'stars_arena' AS project
    , bytearray_ltrim(bytearray_substring(logs.data, 1, 32)) AS trader
    , bytearray_ltrim(bytearray_substring(logs.data, 1 + 32, 32)) AS subject
    , CASE WHEN (varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 2, 32)))) = INT256 '1' THEN 'buy' ELSE 'sell' END AS trade_side
    , CAST(varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 4, 32)))/1e18 AS double) AS amount_original
    , CAST(varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 3, 32))) AS UINT256) AS share_amount
    , CAST(varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 6, 32)))/1e18 AS double) AS subject_fee_amount
    , CAST(varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 7, 32)))/1e18 AS double) AS protocol_fee_amount
    , 0x0000000000000000000000000000000000000000 AS currency_contract
    , 'ETH' AS currency_symbol --this field gets overriden in final social.trades spell
    , CAST(varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 8, 32))) AS UINT256) AS supply
    , txs.hash AS tx_hash
    , logs.index AS evt_index
    , txs.to AS contract_address
FROM {{ source('avalanche_c', 'transactions') }} txs
INNER JOIN {{ source('avalanche_c', 'logs') }} logs ON logs.block_number = txs.block_number
    AND logs.tx_hash = txs.hash
    AND logs.topic0 = 0xc9d4f93ded9b42fa24561e02b2a40f720f71601eb1b3f7b3fd4eff20877639ee
    {% if is_incremental() %}
    AND {{ incremental_predicate('logs.block_time') }}
    {% else %}
    AND logs.block_time >= TIMESTAMP '{{stars_arena_start_date}}'
    {% endif %}
WHERE
    {% if is_incremental() %}
    {{ incremental_predicate('txs.block_time') }}
    {% else %}
    txs.block_time >= TIMESTAMP '{{stars_arena_start_date}}'
    {% endif %}
    AND txs.to = 0xa481b139a1a654ca19d2074f174f17d7534e8cec
    AND txs.success