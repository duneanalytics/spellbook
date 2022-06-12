{{ config(
        alias='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_id'
        )
}}

SELECT blockchain, 'opensea' as project, 'v1' as version, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id, unique_id FROM 
(SELECT blockchain, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id, unique_id FROM {{ ref('opensea_ethereum_trades') }} 
UNION ALL
SELECT blockchain, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id, unique_id FROM {{ ref('opensea_solana_trades') }}) 