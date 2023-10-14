{{ config(
    schema = 'stars_arena_avalanche_c',
    tags = ['dunesql'],
    alias = alias('base_trades')
    )
}}

{% set stars_arena_start_date = '2023-09-20' %}

SELECT txs.block_time
, txs.block_number
, txs."from" AS tx_from
, bytearray_ltrim(bytearray_substring(logs.data, 1, 32)) AS trader
, bytearray_ltrim(bytearray_substring(logs.data, 1 + 32, 32)) AS subject
, (varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 2, 32)))) AS is_buy
, (varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 3, 32)))) AS share_amount
, (varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 4, 32))))/1e18 AS eth_amount
, (varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 5, 32))))/1e18 AS eth_amount
, (varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 6, 32))))/1e18 AS protocol_fee
, (varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 7, 32))))/1e18 AS subject_fee
, (varbinary_to_int256(bytearray_ltrim(bytearray_substring(logs.data, 1 + 32 * 8, 32)))) AS supply
, txs.hash AS tx_hash
, logs.index AS evt_index
FROM {{ source('avalanche_c', 'transactions') }} txs
LEFT JOIN {{ source('avalanche_c', 'logs') }} logs ON logs.block_number = txs.block_number
    AND logs.tx_hash = txs.hash
    AND txs.to = 0xa481b139a1a654ca19d2074f174f17d7534e8cec
    AND txs.success
WHERE logs.topic0 = 0xc9d4f93ded9b42fa24561e02b2a40f720f71601eb1b3f7b3fd4eff20877639ee
    {% if is_incremental() %}
    WHERE txs.block_time >= date_trunc('day', now() - interval '7' day)
    WHERE logs.block_time >= date_trunc('day', now() - interval '7' day)
    {% else %}
    WHERE txs.block_time >= TIMESTAMP '{{stars_arena_start_date}}'
    WHERE logs.block_time >= TIMESTAMP '{{stars_arena_start_date}}'
    {% endif %}