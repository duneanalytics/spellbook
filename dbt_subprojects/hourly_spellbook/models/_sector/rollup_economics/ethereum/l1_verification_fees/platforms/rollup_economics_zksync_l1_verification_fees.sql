{{ config(
    schema = 'rollup_economics_zksync'
    , alias = 'l1_verification_fees' 
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['name', 'tx_hash']
    , post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "rollup_economics",
                                    \'["niftytable", "lgingerich"]\') }}'
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
    , (t.gas_price / 1e18) * t.gas_used AS verification_fee_native
    , {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    , 44*32 / cast(1024 AS double) / cast(1024 AS double) AS proof_size_mb
FROM {{ source('ethereum', 'transactions') }} AS t
WHERE (
    -- L1 transactions settle here pre-Boojum
    t.to = 0x3dB52cE065f728011Ac6732222270b3F2360d919
    -- L1 transactions settle here post-Boojum
    OR t.to = 0xa0425d71cB1D6fb80E65a5361a04096E0672De03
    -- L1 transactions settle here post-EIP4844
    OR t.to = 0xa8CB082A5a689E0d594d7da1E2d72A3D63aDc1bD
)
AND (
    -- L1 transactions use these method ID's pre-Boojum
    bytearray_substring(t.data, 1, 4) = 0x7739cbe7 -- Prove Block
    OR
    -- L1 transactions use these method ID's post-Boojum
    bytearray_substring(t.data, 1, 4) = 0x7f61885c -- Prove Batches
)
AND t.block_time >= TIMESTAMP '2023-03-24' -- ZKsync Era public mainnet launch date
{% if is_incremental() %}
AND {{incremental_predicate('t.block_time')}}
{% endif %}