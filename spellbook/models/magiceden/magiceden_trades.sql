{{ config(
        alias ='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}

SELECT blockchain, 'magiceden' as project, '' as version, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, unique_trade_id 
FROM 
(
SELECT blockchain, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, unique_trade_id
FROM {{ ref('magiceden_solana_trades') }}
) 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time > now() - interval 2 days
{% endif %} 