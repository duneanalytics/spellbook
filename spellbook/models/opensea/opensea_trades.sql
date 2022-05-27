{{ config(
        alias='trades'
        )
}}

SELECT blockchain, tx_hash, block_time, amount_usd, amount, token_symbol, token_address FROM 
(SELECT blockchain, tx_hash, block_time, amount_usd, amount, token_symbol, token_address FROM {{ ref('opensea_ethereum_trades') }} 
UNION ALL
SELECT blockchain, tx_hash, block_time, amount_usd, amount, token_symbol, token_address FROM {{ ref('opensea_solana_trades') }}) 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where block_time > (select max(block_time) from {{ this }})
{% endif %} 