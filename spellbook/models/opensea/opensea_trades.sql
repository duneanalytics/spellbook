{{ config(
        alias='trades'
        )
}}

SELECT blockchain, 'opensea' as project, 'v1' as version, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id FROM 
(SELECT blockchain, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id FROM {{ ref('opensea_ethereum_trades') }} 
UNION ALL
SELECT blockchain, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id FROM {{ ref('opensea_solana_trades') }}) 