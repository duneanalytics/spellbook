{{ config(alias='order_rewards',
        post_hook='{{ expose_spells_hide_trino(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith"]\') }}'
)}}

-- PoC Query here - https://dune.com/queries/1752782
select
    tx_hash,
    solver as solver_address,
    data.amount as cow_reward,
    cast(data.surplus_fee as double) as surplus_fee,
    order_uid
from {{ source('cowswap', 'raw_order_rewards') }}
