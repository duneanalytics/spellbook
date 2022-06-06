{{ config(
        alias='active_traders_day'
        )
}}

SELECT blockchain, day, traders FROM {{ ref('opensea_ethereum_active_traders_day') }} 
UNION ALL
SELECT blockchain, day, traders FROM {{ ref('opensea_solana_active_traders_day') }} 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where day > (select max(day) from {{ this }})
{% endif %} 