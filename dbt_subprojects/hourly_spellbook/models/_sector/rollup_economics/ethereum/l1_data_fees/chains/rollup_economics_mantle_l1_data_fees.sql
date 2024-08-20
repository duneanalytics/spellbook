{{ config(
    schema = 'rollup_economics_mantle'
    , alias = 'l1_data_fees'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['name', 'tx_hash']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

SELECT
    'mantle' AS name
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
    0xD1328C9167e0693B689b5aa5a024379d4e437858 -- Rollup Proxy (last used as of block 19437175)
    , 0x31d543e7BE1dA6eFDc2206Ef7822879045B9f481 -- L2OutputOracle Proxy (used as of block 19440324)
    , 0x50Fa427235C7C8cAA4A0C21b5009f5a0d015B23A -- BVM_EigenDataLayrChain Proxy (DA1) (last used as of block 19437271)
    , 0x5BD63a7ECc13b955C4F57e3F12A64c10263C14c1 -- DataLayrServiceManager Proxy (DA2) (used as of block 19439557)
)
AND bytearray_substring(t.data, 1, 4) IN (
    0x49cd3004 -- createAssertionWithStateBatch
    , 0x9aaab648 -- proposeL2Output
    , 0x5e4a3056 -- storeData (DA1)
    , 0x4618ed87 -- confirmData (DA1)
    , 0x58942e73 -- confirmDataStore (DA2)
    , 0xdcf49ea7 -- initDataStore (DA2)
)
AND t.block_time >= TIMESTAMP '2023-06-27'
{% if is_incremental() %}
AND {{incremental_predicate('t.block_time')}}
{% endif %}