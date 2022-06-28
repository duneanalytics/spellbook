{{ config(
        alias ='trades',
        materialized ='incremental'
        )
}}

SELECT *
FROM 
(
SELECT * FROM {{ ref('opensea_trades') }} 
        UNION ALL
SELECT * FROM {{ ref('magiceden_trades') }}
)

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time > now() - interval 2 days
{% endif %} 