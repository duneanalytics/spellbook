{{config(alias='sandwich_attackers_ethereum')}}

with 
 eth_sandwich_attackers as (
    select 
        distinct buy.tx_to as address
    from {{ref('dex_trades')}} buy
    inner join {{ref('dex_trades')}} sell 
        on sell.block_time = buy.block_time
            and sell.tx_hash != buy.tx_hash
            and buy.`tx_from` = sell.`tx_from`
            and buy.`tx_to` = sell.`tx_to`
            and buy.project_contract_address = sell.project_contract_address
            and buy.amount_usd <= sell.amount_usd
            and buy.token_bought_address = sell.token_sold_address
            and buy.token_sold_address = sell.token_bought_address
            and buy.token_bought_amount_raw = sell.token_sold_amount_raw
    where buy.blockchain = 'ethereum'
        and sell.blockchain = 'ethereum' 
        and buy.tx_to != '0x7a250d5630b4cf539739df2c5dacb4c659f2488d' -- uniswap v2 router
  )
select
  array("ethereum") as blockchain,
  address,
  "Sandwich Attacker" AS name,
  "sandwich_attackers" AS category,
  "alexth" AS contributor,
  "query" AS source,
  timestamp('2022-10-14') as created_at,
  now() as updated_at
from
  eth_sandwich_attackers