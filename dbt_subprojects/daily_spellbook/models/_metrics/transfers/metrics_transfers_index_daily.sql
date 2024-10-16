{{ config(
        schema = 'metrics'
        , alias = 'transfers_index_daily'
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
        sum(net_transfer_amount_usd) as baseline_net_transfer_amount_usd -- sum is required due to blockchain being second unique key in source
    from
        {{ ref('metrics_transfers_daily') }}
    where
        block_date = date '{{ baseline_date }}'
), daily as (
    select
        blockchain
        , block_date
        , net_transfer_amount_usd
    from
        {{ ref('metrics_transfers_daily') }}
    where
        block_date >= date '{{ baseline_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
)
select
    d.blockchain
    , d.block_date
    , d.net_transfer_amount_usd
    , b.baseline_net_transfer_amount_usd
    , (d.net_transfer_amount_usd / b.baseline_net_transfer_amount_usd) * 100 as transfers_index
from
    daily as d
left join
    baseline as b
    on 1 = 1