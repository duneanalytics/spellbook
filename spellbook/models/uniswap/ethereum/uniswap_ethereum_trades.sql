 {{
  config(
        alias='trades'
  )
}}

SELECT blockchain, project, version, block_time, token_a_symbol, token_b_symbol, 
       token_a_amount, token_b_amount, trader_a, trader_b, usd_amount, token_a_address, 
       token_b_address, exchange_contract_address, tx_hash, tx_from, tx_to, trade_id 
FROM (SELECT * FROM {{ ref('uniswap_v2_ethereum_trades') }} 
UNION ALL
SELECT * FROM {{ ref('uniswap_v3_ethereum_trades') }}) 
