 {{
  config(
        alias='trades',        
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
  )
}}

SELECT blockchain, project, version, block_time, token_a_symbol, token_b_symbol, 
       token_a_amount, token_b_amount, trader_a, trader_b, usd_amount, token_a_address, 
       token_b_address, exchange_contract_address, tx_hash, tx_from, tx_to, unique_trade_id
FROM (SELECT * FROM {{ ref('uniswap_v2_ethereum_trades') }} 
UNION ALL
SELECT * FROM {{ ref('uniswap_v3_ethereum_trades') }}) 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time > now() - interval 2 days
{% endif %} 
