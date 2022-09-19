{{config(alias='arb_traders_ethereum')}}


with
  eth_arb_traders as (
    SELECT
      distinct t1.tx_from as address
    FROM
      {{ref('dex_trades')}} t1
      INNER JOIN {{ref('dex_trades')}} t2 ON t1.tx_hash = t2.tx_hash
    WHERE
      t1.blockchain = 'ethereum'
      AND t2.blockchain = 'ethereum'
      -- AND t1.block_date >= date_trunc('day', now() - interval '180 days')
      AND t1.token_sold_address = t2.token_bought_address
      AND t1.token_bought_address = t2.token_sold_address
      AND t1.evt_index != t2.evt_index
    GROUP BY
      t1.tx_from
    ORDER BY
      1
  )
select
  "ethereum" as blockchain,
  address,
  "Arbitrage Trader" AS name,
  "arb_traders" AS category,
  "alexth" AS contributor,
  "query" AS source,
  timestamp('2022-09-18') as created_at,
  now() as updated_at
from
  eth_arb_traders