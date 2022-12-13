{{ config(alias='order_rewards',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith"]\') }}'
)}}

-- PoC Query here - https://dune.com/queries/1752782
select
    tx_hash,
    data.amount as cow_reward,
    cast(data.surplus_fee as double) as surplus_fee,
    order_uid
from {{ source('cowswap', 'raw_order_rewards') }}
