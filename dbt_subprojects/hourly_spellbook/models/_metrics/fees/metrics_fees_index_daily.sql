{{ config(
        schema = 'metrics'
        , alias = 'fees_index_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

{% set baseline_date = '2018-01-01' %}

with baseline as (
    select
        sum(gas_fees_usd) as baseline_gas_fees_usd -- sum is required due to blockchain being second unique key in source
    from
        {{ ref('metrics_gas_fees_daily') }}
    where
        block_date = date '{{ baseline_date }}'
), daily as (
    select
        blockchain
        , block_date
        , gas_fees_usd
    from
        {{ ref('metrics_gas_fees_daily') }}
    where
        block_date >= date '{{ baseline_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
)
select
    d.blockchain
    , d.block_date
    , d.gas_fees_usd
    , b.baseline_gas_fees_usd
    , (d.gas_fees_usd / b.baseline_gas_fees_usd) * 100 as fees_index
from
    daily as d
left join
    baseline as b
    on 1 = 1