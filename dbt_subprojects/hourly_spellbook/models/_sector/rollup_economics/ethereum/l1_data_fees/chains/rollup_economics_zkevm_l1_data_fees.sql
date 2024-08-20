{{ config(
    schema = 'rollup_economics_zkevm'
    , alias = 'l1_data_fees'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['name', 'tx_hash']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

SELECT
    'zkevm' AS name
    , cast(date_trunc('month', t.block_time) AS date) AS block_month
    , cast(date_trunc('day', t.block_time) AS date) AS block_date
    , t.block_time
    , t.block_number
    , t.hash AS tx_hash
    , t.index AS tx_index
    , t.gas_price
    , t.gas_used
    , (t.gas_price / 1e18) * t.gas_used AS data_fee_native
    , {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    , (length(t.data)) AS data_length
FROM {{ source('ethereum', 'transactions') }} t
WHERE t.to IN (
    0x5132a183e9f3cb7c848b0aac5ae0c4f0491b7ab2 -- old proxy
    , 0x519E42c24163192Dca44CD3fBDCEBF6be9130987 -- new proxy (as of block 19218878)
)
AND bytearray_substring(t.data, 1, 4) IN (
    0x5e9145c9 -- sequenceBatches
    , 0xecef3f99 -- sequenceBatches (as of block 19218878)
    , 0xdef57e54 -- sequenceBatches
)
AND t.block_time >= TIMESTAMP '2023-03-01'
{% if is_incremental() %}
AND {{incremental_predicate('t.block_time')}}
{% endif %}