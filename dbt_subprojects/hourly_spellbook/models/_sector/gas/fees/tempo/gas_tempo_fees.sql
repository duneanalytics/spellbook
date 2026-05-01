{% set blockchain = 'tempo' %}
{% set default_fee_token = '0x20c0000000000000000000000000000000000000' %}
{% set default_fee_token_price = 1.0 %}
-- Attodollars to TIP-20 smallest units per Tempo spec: ceil(attodollars / 10^12) via (v + 10^12 - 1) / 10^12.
{% set attodollar_to_token_unit_divisor = 'uint256 \'1000000000000\'' %}

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

WITH base_model as (
    SELECT
        txns.block_time
        ,txns.block_number
        ,txns.hash AS tx_hash
        ,txns."index" AS tx_index
        ,txns."from" AS tx_from
        ,txns.to AS tx_to
        ,cast(gas_price as uint256) as gas_price
        ,txns.gas_used as gas_used
        ,(cast(gas_price as uint256) * cast(txns.gas_used as uint256) + {{attodollar_to_token_unit_divisor}} - uint256 '1') / {{attodollar_to_token_unit_divisor}} as tx_fee_raw
        ,case when txns.priority_fee_per_gas is null or txns.priority_fee_per_gas < 0
            then map(array['base_fee'], array[(cast(gas_price as uint256) * cast(txns.gas_used as uint256) + {{attodollar_to_token_unit_divisor}} - uint256 '1') / {{attodollar_to_token_unit_divisor}}])
            else map(array['base_fee','priority_fee'],
                     array[(cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256) + {{attodollar_to_token_unit_divisor}} - uint256 '1') / {{attodollar_to_token_unit_divisor}}
                            ,(cast(gas_price as uint256) * cast(txns.gas_used as uint256) + {{attodollar_to_token_unit_divisor}} - uint256 '1') / {{attodollar_to_token_unit_divisor}}
                              - (cast(base_fee_per_gas as uint256) * cast(txns.gas_used as uint256) + {{attodollar_to_token_unit_divisor}} - uint256 '1') / {{attodollar_to_token_unit_divisor}}]
                     )
        end as tx_fee_breakdown_raw
        ,blocks.miner AS block_proposer
        ,txns.max_fee_per_gas
        ,txns.priority_fee_per_gas
        ,txns.max_priority_fee_per_gas
        ,blocks.base_fee_per_gas
        ,txns.gas_limit
        ,CASE
            WHEN txns.gas_limit = 0 THEN NULL
            WHEN txns.gas_limit != 0 THEN cast(txns.gas_used as double) / cast(txns.gas_limit as double)
        END AS gas_limit_usage
        ,coalesce(txns.fee_token, {{default_fee_token}}) as fee_token
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

, with_prices as (
    SELECT
        b.block_time
        ,b.block_number
        ,b.tx_hash
        ,b.tx_index
        ,b.tx_from
        ,b.tx_to
        ,b.gas_price
        ,b.gas_used
        ,b.tx_fee_raw
        ,b.tx_fee_breakdown_raw
        ,b.block_proposer
        ,b.max_fee_per_gas
        ,b.priority_fee_per_gas
        ,b.max_priority_fee_per_gas
        ,b.base_fee_per_gas
        ,b.gas_limit
        ,b.gas_limit_usage
        ,b.fee_token
        ,t.symbol as currency_symbol
        ,t.decimals as token_decimals
        ,coalesce(p.price, case when b.fee_token = {{default_fee_token}} then {{default_fee_token_price}} end) as token_price
    FROM base_model b
    LEFT JOIN {{ source('tokens', 'erc20') }} as t
        ON t.blockchain = '{{blockchain}}'
        AND t.contract_address = b.fee_token
    LEFT JOIN {{ source('prices', 'day') }} as p
        ON p.blockchain = '{{blockchain}}'
        AND p.contract_address = b.fee_token
        AND p.timestamp = date_trunc('day', b.block_time)
        {% if is_incremental() -%}
        AND {{ incremental_predicate('p.timestamp') }}
        {%- endif %}
)

SELECT
    '{{blockchain}}' as blockchain
    ,CAST(date_trunc('month', w.block_time) AS DATE) AS block_month
    ,CAST(date_trunc('day', w.block_time) AS DATE) AS block_date
    ,w.block_time
    ,w.block_number
    ,w.tx_hash
    ,w.tx_index
    ,w.tx_from
    ,w.tx_to
    ,w.gas_price
    ,w.gas_used
    ,w.currency_symbol
    ,coalesce(w.tx_fee_raw, uint256 '0') as tx_fee_raw
    ,coalesce(w.tx_fee_raw, uint256 '0') / pow(10, w.token_decimals) as tx_fee
    ,coalesce(w.tx_fee_raw, uint256 '0') / pow(10, w.token_decimals) * w.token_price as tx_fee_usd
    ,transform_values(w.tx_fee_breakdown_raw,
            (k,v) -> coalesce(v, uint256 '0')) as tx_fee_breakdown_raw
    ,transform_values(w.tx_fee_breakdown_raw,
            (k,v) -> coalesce(v, uint256 '0') / pow(10, w.token_decimals)) as tx_fee_breakdown
    ,transform_values(w.tx_fee_breakdown_raw,
            (k,v) -> coalesce(v, uint256 '0') / pow(10, w.token_decimals) * w.token_price) as tx_fee_breakdown_usd
    ,w.fee_token as tx_fee_currency
    ,w.block_proposer
    ,w.max_fee_per_gas
    ,w.priority_fee_per_gas
    ,w.max_priority_fee_per_gas
    ,w.base_fee_per_gas
    ,w.gas_limit
    ,w.gas_limit_usage
FROM with_prices w
