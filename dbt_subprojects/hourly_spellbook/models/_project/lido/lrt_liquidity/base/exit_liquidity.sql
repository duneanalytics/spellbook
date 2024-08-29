{{ config(
        schema='lido_lrt_liquidity_base',
        alias = 'exit_liquidity',
        materialized = 'table',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["base"]\',
                                "project",
                                "lido_lrt_liquidity",
                                \'["pipistrella"]\') }}'
        )
}}


with pools as (
    SELECT distinct blockchain, address, namespace, paired_token, symbol, category
    FROM {{ref('holdings')}}
    WHERE category = 'liquidity_pool' 
)

, tokens(address, symbol, blockchain) as ( -- pared tokens in liquidity pools

   SELECT * FROM (
  values 

     (0x4200000000000000000000000000000000000006, 'WETH', 'base')
   , (0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913, 'USDC', 'base')
   , (0xfde4C96c8593536E31F229EA8f37b2ADa2699bb2, 'USDT', 'base')
   , (0x50c5725949A6F0c72E6C4a641F24049A917DB0Cb, 'DAI', 'base')
   , (0xc1CBa3fCea344f92D9239c08C0568f6F2F0ee452, 'wstETH', 'base')

)
  
)


select  distinct b.day,
        'base' as blockchain,
        b.address,
        h.namespace,
        h.category,
        t.symbol as paired_token_symbol,
        b.balance,
        b.balance_usd
from {{ source('tokens_base', 'balances_daily') }} b  
join tokens t on b.token_address = t.address
join pools h on b.address = h.address  


  