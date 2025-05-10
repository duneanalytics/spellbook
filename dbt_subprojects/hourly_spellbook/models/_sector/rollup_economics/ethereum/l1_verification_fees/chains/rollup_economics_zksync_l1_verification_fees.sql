{{ config(
    schema = 'rollup_economics_zksync'
    , alias = 'l1_verification_fees' 
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
        , (t.gas_price / 1e18) * t.gas_used AS verification_fee_native
        , {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
        , 44*32 / cast(1024 AS double) / cast(1024 AS double) AS proof_size_mb
        , CASE -- If prior to shared bridge, chain_id is always equal to 324 for zksync era. If on shared bridge, fetch chain_id.
            WHEN bytearray_substring(t.data, 1, 4) IN (
                0x7739cbe7 -- Prove Block, pre-Boojum
                , 0x7f61885c -- Prove Batches, post-Boojum
            ) THEN 324
            WHEN bytearray_substring(t.data, 1, 4) IN (
                0xc37533bb -- Prove Batches Shared Bridge, post-v24
            ) THEN varbinary_to_uint256(varbinary_substring(t.data, 5, 32))
            ELSE 324
        END AS chain_id           
    FROM {{ source('ethereum', 'transactions') }} AS t
    WHERE t.to IN (
        0x3dB52cE065f728011Ac6732222270b3F2360d919 -- L1 transactions settle here pre-Boojum
        , 0xa0425d71cB1D6fb80E65a5361a04096E0672De03 -- L1 transactions settle here post-Boojum
        , 0xa8CB082A5a689E0d594d7da1E2d72A3D63aDc1bD -- L1 transactions settle here post-EIP4844
        , 0x5D8ba173Dc6C3c90C8f7C04C9288BeF5FDbAd06E -- L1 transactions settle here post v24 upgrade (shared bridge)
        , 0x3527439923a63F8C13CF72b8Fe80a77f6e572092 -- Validator
        , 0x8c0Bfc04AdA21fd496c55B8C50331f904306F564 -- ValidatorTimelock
    )
    AND bytearray_substring(t.data, 1, 4) IN (
        0x7739cbe7 -- Prove Block, pre-Boojum
        , 0x7f61885c -- Prove Batches, post-Boojum
        , 0xc37533bb -- Prove Batches, post v24 upgrade (shared bridge)
        , 0xe12a6137 -- proveBatchesSharedBridge (Validator)
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
    , verification_fee_native
    , calldata_gas_used
    , proof_size_mb
FROM data
WHERE chain_id = 324 -- zksync era
