{{ config(
        alias='trades'
        )
}}

SELECT blockchain, project, version, block_time, token_a_symbol, token_b_symbol, 
        amount_usd, trader_a, trader_b, exchange_contract_address, tx_hash, trade_id 
FROM (SELECT  blockchain, project, version, block_time, token_a_symbol, token_b_symbol, 
        amount_usd, trader_a, trader_b, exchange_contract_address, tx_hash, trade_id FROM {{ ref('uniswap_ethereum_trades') }} 
UNION ALL
SELECT  blockchain, project, version, block_time, token_a_symbol, token_b_symbol, 
        amount_usd, trader_a, trader_b, exchange_contract_address, tx_hash, trade_id FROM {{ ref('serum_solana_trades') }}) 