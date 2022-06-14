{{ config(
        alias='volume_day'
        )
}}

SELECT blockchain, day, volume, token_symbol, volume_usd FROM {{ ref('opensea_ethereum_volume_day') }} 
UNION ALL
SELECT blockchain, day, volume, token_symbol, volume_usd FROM {{ ref('opensea_solana_volume_day') }} 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where day > (select max(day) from {{ this }})
{% endif %} 