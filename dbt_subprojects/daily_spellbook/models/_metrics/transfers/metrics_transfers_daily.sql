{{ config(
        schema = 'metrics'
        , alias = 'transfers_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with evm as (
    select 
        blockchain
        , block_date
        , sum(transfer_amount_usd_sent) as transfer_amount_usd_sent
        , sum(transfer_amount_usd_received) as transfer_amount_usd_received
        , sum(net_transfer_amount_usd) as net_transfer_amount_usd
    from
        {{ ref('metrics_net_transfers') }}
    where
        1 = 1
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
), solana as (
    select 
        blockchain
        , block_date
        , sum(transfer_amount_usd_sent) as transfer_amount_usd_sent
        , sum(transfer_amount_usd_received) as transfer_amount_usd_received
        , sum(net_transfer_amount_usd) as net_transfer_amount_usd
    from
        {{ ref('metrics_net_solana_transfers') }}
    where
        1 = 1
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
)
select 
    * 
from
    evm
union all
select
    * 
from
    solana