{{ config(
        alias='trades',
        materialized ='incremental'
        )
}}

SELECT blockchain, project, version, block_time, token_a_symbol, token_b_symbol, 
        amount_usd, trader_a, trader_b, exchange_contract_address, tx_hash, unique_trade_id 
FROM (SELECT * FROM {{ ref('serum_v2_solana_trades') }} 
UNION ALL
SELECT * FROM {{ ref('serum_v3_solana_trades') }}) 