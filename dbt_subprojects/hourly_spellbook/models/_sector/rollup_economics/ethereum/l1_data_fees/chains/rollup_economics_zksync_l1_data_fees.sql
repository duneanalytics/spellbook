{{ config(
    schema = 'rollup_economics_zksync'
    , alias = 'l1_data_fees'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['name', 'tx_hash']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

SELECT
    'zksync' AS name
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
    0x3dB52cE065f728011Ac6732222270b3F2360d919 -- L1 transactions settle here pre-Boojum
    , 0xa0425d71cB1D6fb80E65a5361a04096E0672De03 -- L1 transactions settle here post-Boojum
    , 0xa8CB082A5a689E0d594d7da1E2d72A3D63aDc1bD -- L1 transactions settle here post-EIP4844
)
AND bytearray_substring(t.data, 1, 4) IN (
    0x0c4dd810 -- Commit Block, pre-Boojum
    , 0xce9dcf16 -- Execute Block, pre-Boojum
    , 0x701f58c5 -- Commit Batches, post-Boojum
    , 0xc3d93e7c -- Execute Batches, post-Boojum
)
AND t.block_time >= TIMESTAMP '2023-02-14'
{% if is_incremental() %}
AND {{incremental_predicate('t.block_time')}}
{% endif %}