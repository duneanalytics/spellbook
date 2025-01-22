{{ config(alias='order_rewards',
        
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "cow_protocol",
                                    \'["bh2smith"]\') }}'
)}}

-- PoC Query here - https://dune.com/queries/1752782
SELECT
  DISTINCT from_hex(order_uid) as order_uid,
  block_number,
  from_hex(tx_hash) as tx_hash,
  from_hex(solver) as solver,
  from_hex(data.quote_solver) as quote_solver,
  cast(data.amount as uint256)  AS cow_reward,
  cast(data.surplus_fee as uint256) AS surplus_fee
FROM {{ source('cowswap', 'raw_order_rewards') }}
