{{ config(
    schema = 'rollup_economics_imx'
    , alias = 'l1_data_fees'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['name', 'tx_hash']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

SELECT
    'imx' AS name
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
    0x5FDCCA53617f4d2b9134B29090C87D01058e27e9
    , 0x16BA0f221664A5189cf2C1a7AF0d3AbFc70aA295
)
AND bytearray_substring(t.data, 1, 4) IN (
    0x538f9406 -- StateUpdate
    , 0x504f7f6f -- Verify Availability Proof
)
AND t.block_time >= TIMESTAMP '2021-03-24'
{% if is_incremental() %}
AND {{incremental_predicate('t.block_time')}}
{% endif %}