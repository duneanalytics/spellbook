{{ config(
        schema = 'metrics'
        , alias = 'transfers_index_daily'
        , materialized = 'view'
        )
}}

{% set baseline_date = '2015-08-21' %}

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
