{{ config(
        alias ='burns',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}


SELECT * FROM
(SELECT * FROM {{ ref('opensea_burns') }} 
UNION
SELECT * FROM {{ ref('looksrare_ethereum_burns') }})
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time > now() - interval 2 days
{% endif %} 