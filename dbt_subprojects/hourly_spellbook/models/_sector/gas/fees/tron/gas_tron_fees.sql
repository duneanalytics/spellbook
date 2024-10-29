{{ config(
    schema = 'gas_tron
    ,alias = 'fees'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy='merge'
    ,unique_key = ['block_month', 'tx_hash']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH base_model as (
    SELECT
        txns.block_time
        ,txns.block_number
        ,txns.hash AS tx_hash
        ,txns."from" AS tx_from
        ,txns.to AS tx_to
        ,cast(gas_price as uint256) as gas_price
        ,txns.gas_used as gas_used
        ,gas_used * gas_price as tx_fee_raw
        ,{{ var('ETH_ERC20_ADDRESS') }} as tx_fee_currency
        ,txns.gas_limit
        ,CASE  --safe divide-by-zero
            WHEN txns.gas_limit = 0 THEN NULL
            WHEN txns.gas_limit != 0 THEN cast(txns.gas_used as double) / cast(txns.gas_limit as double)
        END AS gas_limit_usage
    FROM {{ source( 'tron', 'transactions') }} txns
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('txns.block_time') }}
    {% endif %}

SELECT
    'tron' as blockchain
    ,CAST(date_trunc('month', block_time) AS DATE) AS block_month
    ,CAST(date_trunc('day', block_time) AS DATE) AS block_date
    ,block_time
    ,block_number
    ,tx_hash
    ,tx_from
    ,tx_to
    ,gas_price
    ,gas_used
    ,p.symbol as currency_symbol
    ,coalesce(tx_fee_raw, 0) as tx_fee_raw
    ,coalesce(tx_fee_raw, 0) / pow(10,6) as tx_fee
    ,coalesce(tx_fee_raw, 0) / pow(10,6) * p.price as tx_fee_usd
    ,cast(null as MAP) as tx_fee_breakdown_raw
    ,cast(null as MAP) as tx_fee_breakdown
    ,cast(null as MAP) as tx_fee_breakdown_usd
    ,tx_fee_currency
    ,cast(null as varbinary) as block_proposer
    ,cast(null as uint256) as gas_limit
    ,cast(null as double) as gas_limit_usage
FROM base_model
LEFT JOIN {{ref('prices_usd_with_native')}} p
    ON p.blockchain = null
    AND symbol = 'TRX'
    AND p.minute = date_trunc('minute', block_time)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}

