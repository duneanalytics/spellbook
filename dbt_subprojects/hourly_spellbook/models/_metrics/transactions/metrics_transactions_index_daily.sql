{{ config(
        schema = 'metrics'
        , alias = 'transactions_index_daily'
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
        sum(tx_count) as baseline_tx_count -- sum is required due to blockchain being second unique key in source
    from
        {{ ref('metrics_transactions_daily') }}
    where
        block_date = date '{{ baseline_date }}'
), daily as (
    select
        blockchain
        , block_date
        , tx_count
    from
        {{ ref('metrics_transactions_daily') }}
    where
        block_date >= date '{{ baseline_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
)
select
    d.blockchain
    , d.block_date
    , d.tx_count
    , b.baseline_tx_count
    , (cast(d.tx_count as double) / cast(b.baseline_tx_count as double)) * 100 as tx_index
from
    daily as d
left join
    baseline as b
    on 1 = 1