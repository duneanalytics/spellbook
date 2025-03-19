{{ config(
        schema = 'metrics'
        , alias = 'dune_index_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

{% set baseline_date = '2018-01-01' %}
{% set start_date = '2015-08-21' %}

with
fees as (
    select
        blockchain
        , block_date
        , gas_fees_usd
        , (gas_fees_usd / (select sum(gas_fees_usd) from {{ ref('metrics_gas_fees_daily') }} where block_date = date '{{ baseline_date }}')) * 10 as fees_index
    from
        {{ ref('metrics_gas_fees_daily') }}
    where
        block_date >= date '{{ start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
),
transactions as (
    select
        blockchain
        , block_date
        , tx_count
        , (tx_count / cast((select sum(tx_count) from {{ ref('metrics_transactions_daily') }} where block_date = date '{{ baseline_date }}') as double)) * 10 as tx_index
    from
        {{ ref('metrics_transactions_daily') }}
    where
        block_date >= date '{{ start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
)
,transfers as (
    select
        blockchain
        , block_date
        , net_transfer_amount_usd
        , (net_transfer_amount_usd / cast((select sum(net_transfer_amount_usd) from {{ ref('metrics_transfers_daily') }} where block_date = date '{{ baseline_date }}') as double)) * 10 as transfers_index
    from
        {{ ref('metrics_transfers_daily') }}
    where
        block_date >= date '{{ start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
)

select
    blockchain
    , block_date
    , 0.45 * coalesce(fees_index,0) + 0.45 * coalesce(transfers_index,0) + 0.10 * coalesce(tx_index,0) as dune_index
    , coalesce(fees_index,0) as fees_index
    , coalesce(transfers_index,0) as transfers_index
    , coalesce(tx_index,0) as tx_index
    , gas_fees_usd
    , tx_count
    , net_transfer_amount_usd
from fees
left join transfers using (blockchain, block_date)
left join transactions using (blockchain, block_date)