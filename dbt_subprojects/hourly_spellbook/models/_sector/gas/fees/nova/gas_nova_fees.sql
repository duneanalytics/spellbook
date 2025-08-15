{% set blockchain = 'nova' %}

{{ config(
    schema = 'gas_' + blockchain
    ,alias = 'fees'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy='merge'
    ,unique_key = ['block_month', 'tx_hash']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set test_short_ci=false %}

WITH native_token_prices as (
    {{ native_token_prices(blockchain) }}
)
, base_model as (
    SELECT
        txns.block_time
        ,txns.block_number
        ,txns.hash AS tx_hash
        ,txns."from" AS tx_from
        ,txns.to AS tx_to
        ,cast(gas_price as uint256) as gas_price
        ,txns.gas_used as gas_used
        ,cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256) as tx_fee_raw
        ,map(array['base_fee'], array[cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256)]) as tx_fee_breakdown_raw
        ,blocks.miner AS block_proposer
        ,txns.max_fee_per_gas
        ,txns.priority_fee_per_gas
        ,txns.max_priority_fee_per_gas
        ,blocks.base_fee_per_gas
        ,txns.gas_limit
        ,CASE  --safe divide-by-zero
            WHEN txns.gas_limit = 0 THEN NULL
            WHEN txns.gas_limit != 0 THEN cast(txns.gas_used as double) / cast(txns.gas_limit as double)
        END AS gas_limit_usage
    FROM {{ source(blockchain, 'transactions') }} txns
    INNER JOIN {{ source(blockchain, 'blocks') }} blocks
        ON txns.block_number = blocks.number
        {% if is_incremental() %}
        AND {{ incremental_predicate('blocks.time') }}
        {% endif %}
    {% if test_short_ci %}
    WHERE {{ incremental_predicate('txns.block_time') }}
    OR txns.hash in (select tx_hash from {{ref('evm_gas_fees')}})
    {% elif is_incremental() %}
    WHERE {{ incremental_predicate('txns.block_time') }}
    {% endif %}
)

SELECT
    '{{blockchain}}' as blockchain
    ,CAST(date_trunc('month', b.block_time) AS DATE) AS block_month
    ,CAST(date_trunc('day', b.block_time) AS DATE) AS block_date
    ,b.block_time
    ,b.block_number
    ,b.tx_hash
    ,b.tx_from
    ,b.tx_to
    ,b.gas_price
    ,b.gas_used
    ,p.symbol as currency_symbol
    ,coalesce(b.tx_fee_raw, 0) as tx_fee_raw
    ,coalesce(b.tx_fee_raw, 0) / pow(10,p.decimals) as tx_fee
    ,coalesce(b.tx_fee_raw, 0) / pow(10,p.decimals) * p.price as tx_fee_usd
    ,transform_values(b.tx_fee_breakdown_raw,
            (k,v) -> coalesce(v, cast(0 as uint256))) as tx_fee_breakdown_raw
    ,transform_values(b.tx_fee_breakdown_raw,
            (k,v) -> coalesce(v, cast(0 as uint256)) / pow(10,p.decimals) ) as tx_fee_breakdown
    ,transform_values(b.tx_fee_breakdown_raw,
            (k,v) -> coalesce(v, cast(0 as uint256)) / pow(10,p.decimals) * p.price) as tx_fee_breakdown_usd
    ,p.contract_address as tx_fee_currency
    ,b.block_proposer
    ,b.max_fee_per_gas
    ,b.priority_fee_per_gas
    ,b.max_priority_fee_per_gas
    ,b.base_fee_per_gas
    ,b.gas_limit
    ,b.gas_limit_usage
FROM base_model as b
LEFT JOIN native_token_prices as p
    ON p.timestamp = date_trunc('day', b.block_time)