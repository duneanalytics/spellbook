{{config(alias = alias('sandwich_attackers_ethereum'))}}

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
            and buy.token_bought_address = sell.token_sold_address
            and buy.token_sold_address = sell.token_bought_address
            and buy.token_bought_amount_raw = sell.token_sold_amount_raw
    inner join {{source('ethereum', 'transactions')}} et_buy
        on et_buy.hash = buy.tx_hash
    inner join {{source('ethereum','transactions')}} et_sell
        on et_sell.hash = sell.tx_hash
    where 
        buy.blockchain = 'ethereum'
        and sell.blockchain = 'ethereum'
        and (et_sell.index >= et_buy.index + 2 -- buy first
        or et_buy.index >= et_sell.index + 2) -- sell first
        and buy.tx_to != '0x7a250d5630b4cf539739df2c5dacb4c659f2488d' -- uniswap v2 router 
        and buy.tx_to != '0x68b3465833fb72a70ecdf485e0e4c7bd8665fc45' -- uniswap v3 router
  )
select
  "ethereum" as blockchain,
  address,
  "Sandwich Attacker" AS name,
  "dex" AS category,
  "alexth" AS contributor,
  "query" AS source,
  timestamp('2022-10-14') as created_at,
  now() as updated_at,
  "sandwich_attackers" as model_name,
   "persona" as label_type
from
  eth_sandwich_attackers