{{ config(
        alias ='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_trade_id'
        )
}}

SELECT *
FROM
(
       SELECT *
       FROM {{ ref('uniswap_v1_ethereum_trades') }}
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        WHERE block_time > now() - interval 2 days
        {% endif %} 
       -- UNION
       -- SELECT *
       -- FROM {{ ref('uniswap_v2_ethereum_trades') }}
       --  {% if is_incremental() %}
       --  -- this filter will only be applied on an incremental run
       --  WHERE block_time > now() - interval 2 days
       --  {% endif %} 
       -- UNION
       -- SELECT *
       -- FROM {{ ref('uniswap_v3_ethereum_trades') }}
       --  {% if is_incremental() %}
       --  -- this filter will only be applied on an incremental run
       --  WHERE block_time > now() - interval 2 days
       --  {% endif %} 
)