{{ config(alias='internal_imbalances',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith"]\') }}'
)}}

-- PoC Query here - https://dune.com/queries/2497645?d=11
select
    block_number,
    from_hex(token) as token,
    from_hex(tx_hash) as tx_hash,
    cast(amount as int256) as amount
from {{ source('cowswap', 'raw_internal_imbalance') }}
