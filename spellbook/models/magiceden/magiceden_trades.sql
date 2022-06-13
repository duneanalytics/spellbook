{{ config(
        alias='trades'
        )
}}

SELECT blockchain, 'magiceden' as project, '' as version, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id FROM 
(
        SELECT blockchain, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id 
        FROM {{ ref('magiceden_solana_trades') }}
) 