{{ config(
        alias ='events',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}

SELECT * FROM (
SELECT * FROM {{ ref('opensea_events') }} 
         UNION
SELECT * FROM {{ ref('magiceden_events') }}
         UNION
SELECT * FROM {{ ref('looksrare_ethereum_events') }}
         UNION
SELECT * FROM {{ ref('x2y2_ethereum_events') }}
)

{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE block_time > now() - interval 2 days
{% endif %} 