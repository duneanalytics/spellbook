{{ config(
    schema = 'rocketpool_ethereum',
    alias = 'minipool_master',
    materialized = 'table',
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "rocketpool",
                                \'["mtitus6"]\') }}'
    )
}}

with minipool_deposits as (
    select
        pubkey,
        minipool,
        bond_amount,
        node_fee,
        'standard' as deposit_type
    from {{ ref('rocketpool_minipool_deposit_standard') }}

    union

    select
        pubkey,
        minipool,
        bond_amount,
        node_fee,
        'credit' as deposit_type
    from {{ ref('rocketpool_minipool_deposit_credit') }}

    union

    select
        pubkey,
        minipool,
        bond_amount,
        node_fee,
        'vacant' as deposit_type
    from {{ ref('rocketpool_minipool_deposit_vacant') }}
)
select
    minipool.minipool,
    minipool.created_time,
    minipool.destroyed_time,
    minipool.node_address,
    deposits.deposit_type,
    deposits.bond_amount as orig_bond_amount,
    deposits.node_fee as orig_node_fee,
    queue.enqueued_time,
    queue.dequeued_time,
    queue.queue_days,
    queue.queue_hrs,
    beacon_dep.beacon_amount_deposited,
    pubkey.pubkey,
    pubkey.validator_index,
    coalesce(reductions.new_bond_amount, deposits.bond_amount) as bond_amount,
    coalesce(reductions.new_node_fee, deposits.node_fee) as node_fee,
    reductions.new_bond_amount is not null as bond_reduced,
    beacon_wth.exited,
    beacon_wth.beacon_amount_withdrawn,
    beacon_wth.beacon_amount_skim_withdrawn,
    coalesce(dist.is_distributed, false) as is_distributed
from {{ ref('rocketpool_minipool_created_destroyed') }} as minipool
left join minipool_deposits  as deposits
    on minipool.minipool = deposits.minipool
left join {{ ref('rocketpool_minipool_bond_reduction') }} as reductions
    on minipool.minipool = reductions.minipool
left join {{ ref('rocketpool_minipool_pubkey_index') }} as pubkey
    on minipool.minipool = pubkey.minipool
left join {{ ref('rocketpool_minipool_beacon_deposit') }} as beacon_dep
    on pubkey.pubkey = beacon_dep.pubkey
left join {{ ref('rocketpool_minipool_beacon_withdrawal') }} as beacon_wth
    on deposits.minipool = beacon_wth.minipool
left join {{ ref('rocketpool_minipool_balance_distribution') }} as dist
    on minipool.minipool = dist.minipool
left join {{ ref('rocketpool_minipool_queue') }} as queue
    on minipool.minipool = queue.minipool
