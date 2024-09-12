{{ config(
    schema = 'rollup_economics_zksync_lite'
    , alias = 'l1_data_fees'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['name', 'tx_hash']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

SELECT
    'zksync_lite' AS name
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
    0xabea9132b05a70803a4e85094fd0e1800777fbef
)
AND t."from" IN (
    0xda7357bbce5e8c616bc7b0c3c86f0c71c5b4eabb -- Old L2 Operator
    , 0x18c208921F7a741510a7fc0CfA51E941735DAE54 -- L2 Operator
    , 0x01c3a1a6890a146ac187a019f9863b3ab2bff91e -- L2 Operator V1
)
AND bytearray_substring(t.data, 1, 4) IN (
    0x45269298 -- Commit Block
)
AND t.block_time >= TIMESTAMP '2021-02-09'
{% if is_incremental() %}
AND {{incremental_predicate('t.block_time')}}
{% endif %}