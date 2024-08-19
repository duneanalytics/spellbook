{{ config(
    schema = 'rollup_economics_optimism'
    , alias = 'l1_data_fees'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['name', 'tx_hash']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

SELECT
    -- 'optimism' AS name
    lower(protocol_name) AS name
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
FROM (
    SELECT 
        protocol_name, t.block_time, t.block_number, t.hash, t.index, t.gas_used, t.gas_price, t.data
    FROM {{ source('ethereum','transactions') }} as t
    INNER JOIN {{ source('addresses_ethereum','optimism_batchinbox_combinations') }} as op
        ON t."from" = op.l1_batch_inbox_from_address
        AND t.to = op.l1_batch_inbox_to_address
    WHERE t.block_time >= timestamp '2020-01-01'
    
    UNION ALL
    
    SELECT 
        protocol_name, t.block_time, t.block_number, t.hash, t.index, t.gas_used, t.gas_price, t.data
    FROM {{ source('ethereum','transactions') }} as t
    INNER JOIN {{ source('addresses_ethereum','optimism_outputoracle_combinations') }} as op
        ON t."from" = op.l2_output_oracle_from_address
        AND t.to = op.l2_output_oracle_to_address
    WHERE t.block_time >= timestamp '2020-01-01'
) t
{% if is_incremental() %}
WHERE {{incremental_predicate('t.block_time')}}
{% endif %}