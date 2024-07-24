{{ config(
    schema = 'rollup_economics_zksync',
    alias = 'l1_data_fees',    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'tx_hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
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
    , (t.gas_price / 1e18) * t.gas_used AS data_fee_native
    , {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    , (length(t.data)) AS data_length
FROM {{ source('ethereum', 'transactions') }} t
WHERE (
    -- L1 transactions settle here pre-Boojum
    t.to = 0x3dB52cE065f728011Ac6732222270b3F2360d919
    OR 
    -- L1 transactions settle here post-Boojum
    t.to = 0xa0425d71cB1D6fb80E65a5361a04096E0672De03
    OR
    -- L1 transactions settle here post-EIP4844
    t.to = 0xa8CB082A5a689E0d594d7da1E2d72A3D63aDc1bD
)
AND (
    -- L1 transactions use these method ID's pre-Boojum
    bytearray_substring(t.data, 1, 4) = 0x0c4dd810 -- Commit Block
    OR
    bytearray_substring(t.data, 1, 4) = 0xce9dcf16 -- Execute Block
    OR
    -- L1 transactions use these method ID's post-Boojum
    bytearray_substring(t.data, 1, 4) = 0x701f58c5 -- Commit Batches
    OR
    bytearray_substring(t.data, 1, 4) = 0xc3d93e7c -- Execute Batches
)
AND t.block_time >= TIMESTAMP '2023-03-24' -- ZKsync Era public mainnet launch date
{% if is_incremental() %}
AND {{incremental_predicate('t.block_time')}}
{% endif %}