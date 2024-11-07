{{ config(
        schema = 'metrics'
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

select
    blockchain
    , block_date
    , transfer_amount_usd_sent
    , transfer_amount_usd_received
    , transfer_amount_usd
    , net_transfer_amount_usd
from
    {{ ref('metrics_net_transfers_daily') }}
union all
select
    blockchain
    , block_date
    , transfer_amount_usd_sent
    , transfer_amount_usd_received
    , transfer_amount_usd
    , net_transfer_amount_usd
from
    {{ ref('metrics_net_solana_transfers_daily') }}
