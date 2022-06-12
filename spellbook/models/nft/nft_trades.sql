{{ config(
        alias='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_id'
        )
}}

SELECT blockchain, project, version, tx_hash, block_time, amount_usd, amount, token_symbol, token_address,trade_id, unique_id FROM 
(SELECT blockchain, project, version, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id, unique_id FROM {{ ref('opensea_trades') }} 
UNION ALL
SELECT blockchain, project, version, tx_hash, block_time, amount_usd, amount, token_symbol, token_address, trade_id, unique_id FROM {{ ref('magiceden_trades') }}) 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where block_time > (select max(block_time) from {{ this }})
{% endif %} 