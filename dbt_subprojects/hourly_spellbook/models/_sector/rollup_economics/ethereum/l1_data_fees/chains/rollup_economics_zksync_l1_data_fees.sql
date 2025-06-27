{{ config(
    schema = 'rollup_economics_zksync'
    , alias = 'l1_data_fees'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['name', 'tx_hash']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

WITH data AS (
    SELECT
        cast(date_trunc('month', t.block_time) AS date) AS block_month
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
        , CASE -- If prior to shared bridge, chain_id is always equal to 324 for zksync era. If on shared bridge, fetch chain_id.
            WHEN bytearray_substring(t.data, 1, 4) IN (
                0x0c4dd810 -- Commit Block, pre-Boojum
                , 0x701f58c5 -- Commit Batches, post-Boojum
                , 0xce9dcf16 -- Execute Block, pre-Boojum
                , 0xc3d93e7c -- Execute Batches, post-Boojum
            ) THEN 324
            WHEN bytearray_substring(t.data, 1, 4) IN (
                0x6edd4f12 -- Commit Batches Shared Bridge, post-v24
                , 0x6f497ac6 -- Execute Batches Shared Bridge, post-v24
            ) THEN varbinary_to_uint256(varbinary_substring(t.data, 5, 32))
            ELSE 324
        END AS chain_id        
    FROM {{ source('ethereum', 'transactions') }} t
    WHERE t.to IN (
        0x3dB52cE065f728011Ac6732222270b3F2360d919 -- L1 transactions settle here pre-Boojum
        , 0xa0425d71cB1D6fb80E65a5361a04096E0672De03 -- L1 transactions settle here post-Boojum
        , 0xa8CB082A5a689E0d594d7da1E2d72A3D63aDc1bD -- L1 transactions settle here post-EIP4844
        , 0x5D8ba173Dc6C3c90C8f7C04C9288BeF5FDbAd06E -- L1 transactions settle here post v24 upgrade (shared bridge)
        , 0x0D3250c3D5FAcb74Ac15834096397a3Ef790ec99 -- Batcher
        , 0x3527439923a63F8C13CF72b8Fe80a77f6e572092 -- Validator
        , 0x8c0Bfc04AdA21fd496c55B8C50331f904306F564 -- ValidatorTimelock
    )
    AND bytearray_substring(t.data, 1, 4) IN (
        0x0c4dd810 -- Commit Block, pre-Boojum
        , 0xce9dcf16 -- Execute Block, pre-Boojum
        , 0x701f58c5 -- Commit Batches, post-Boojum
        , 0xc3d93e7c -- Execute Batches, post-Boojum
        , 0x6edd4f12 -- Commit Batches, post v24 upgrade (shared bridge)
        , 0x6f497ac6 -- Execute Batches, post v24 upgrade (shared bridge)
        , 0xcf02827d -- executeBatchesSharedBridge (Validator)
        , 0x98f81962 -- commitBatchesSharedBridge (Batcher)
    )
    AND t.block_time >= TIMESTAMP '2023-02-14'
    {% if is_incremental() %}
    AND {{incremental_predicate('t.block_time')}}
    {% endif %}
)

SELECT
    'zksync era' AS name
    , block_month
    , block_date
    , block_time
    , block_number
    , tx_hash
    , tx_index
    , gas_price
    , gas_used
    , data_fee_native
    , calldata_gas_used
    , data_length
FROM data
WHERE chain_id = 324 -- zksync era
