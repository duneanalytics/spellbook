{{ config(
        alias='txns_day'
        )
}}


SELECT blockchain, day, transactions FROM 
(SELECT blockchain, day, transactions FROM {{ ref('opensea_ethereum_txns_day') }} 
UNION ALL
SELECT blockchain, day, transactions FROM {{ ref('opensea_solana_txns_day') }}) 
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
where day > (select max(day) from {{ this }})
{% endif %} 