{% set blockchain = 'celo' %}

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

{% set test_short_ci = false %}

WITH

fee_currency_wrapper_map (fee_currency_wrapper_contract, wrapped_token_contract, symbol) AS (
    values
    (0x0e2a3e05bc9a16f5292a6170456a710cb89c6f72, 0x48065fbBE25f71C9282ddf5e1cD6D6A887483D5e, 'USDT'),
    (0x2f25deb3848c207fc8e0c34035b3ba7fc157602b, 0xcebA9300f2b948710d2653dD7B07f33A8B32118C, 'USDC')
),

base_model AS (
    SELECT
        txns.block_time
        ,txns.block_number
        ,txns.hash AS tx_hash
        ,txns."from" AS tx_from
        ,txns.to AS tx_to
        ,cast(gas_price as uint256) as gas_price
        ,txns.gas_used as gas_used
        ,cast(gas_price as uint256) * cast(txns.gas_used as uint256) as tx_fee_raw
        ,map_concat(
            map()
            ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
                then map(array['base_fee'], array[(cast(gas_price as uint256) * cast(txns.gas_used as uint256))])
                else map(array['base_fee','priority_fee'],
                         array[(cast(gas_price - priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))
                                ,(cast(priority_fee_per_gas as uint256) * cast(txns.gas_used as uint256))]
                         )
                end
        ) as tx_fee_breakdown_raw
        ,coalesce(fcwp.wrapped_token_contract, txns.fee_currency, {{var('ETH_ERC20_ADDRESS')}}) as tx_fee_currency
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
    LEFT JOIN fee_currency_wrapper_map fcwp on txns.fee_currency = fcwp.fee_currency_wrapper_contract
    {% if test_short_ci %}
    WHERE {{ incremental_predicate('txns.block_time') }}
    OR txns.hash in (select tx_hash from {{ref('evm_gas_fees')}})
    {% elif is_incremental() %}
    WHERE {{ incremental_predicate('txns.block_time') }}
    {% endif %}
)
SELECT
    '{{blockchain}}' as blockchain
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
    ,coalesce(tx_fee_raw, 0) / pow(10,p.decimals) as tx_fee
    ,coalesce(tx_fee_raw, 0) / pow(10,p.decimals) * p.price as tx_fee_usd
    ,transform_values(tx_fee_breakdown_raw,
            (k,v) -> coalesce(v,0)) as tx_fee_breakdown_raw
    ,transform_values(tx_fee_breakdown_raw,
            (k,v) -> coalesce(v, 0) / pow(10,p.decimals) ) as tx_fee_breakdown
    ,transform_values(tx_fee_breakdown_raw,
            (k,v) -> coalesce(v, 0) / pow(10,p.decimals) * p.price) as tx_fee_breakdown_usd
    ,tx_fee_currency
    ,block_proposer
    ,max_fee_per_gas
    ,priority_fee_per_gas
    ,max_priority_fee_per_gas
    ,base_fee_per_gas
    ,gas_limit
    ,gas_limit_usage
FROM base_model
LEFT JOIN {{ ref('prices_usd_with_native') }} p
    ON p.blockchain = '{{blockchain}}'
    AND p.contract_address = tx_fee_currency
    AND p.minute = date_trunc('minute', block_time)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}