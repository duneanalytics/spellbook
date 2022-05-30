{{ config(
        alias='trades'
        )
}}

SELECT blockchain, 'magiceden' as project, '' as version, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id FROM 
(
        SELECT blockchain, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id 
        FROM {{ ref('magiceden_solana_trades') }}
) 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where block_time > (select max(block_time) from {{ this }})
{% endif %} 