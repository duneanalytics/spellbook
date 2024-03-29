{{
  config(
    alias='ccip_reward_daily',
    partition_by = ['date_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['date_start', 'token']
  )
}}

{% set incremental_interval = '7' %}

WITH
    token_meta AS (
        SELECT
            token_contract,
            token_symbol
        FROM
            {{ref('chainlink_arbitrum_ccip_token_meta')}}
    ),
    token_usd_daily AS (
        SELECT
            cast(date_trunc('day', price.minute) as date) as "date_start",
            token_meta.token_symbol as symbol,
            MAX(price.price) as usd_amount
        FROM
            {{ source('prices', 'usd') }} price
        JOIN token_meta ON price.symbol = token_meta.token_symbol
        {% if is_incremental() %}
            WHERE price.minute >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
        {% endif %} 
        GROUP BY
            1, token_meta.token_symbol
        ORDER BY
            1
    ),
    ccip_reward_daily AS (
        SELECT 
            ccip_send_requested_daily.date_start,
            cast(date_trunc('month', ccip_send_requested_daily.date_start) as date) as date_month,  
            SUM(ccip_send_requested_daily.fee_amount) as token_amount,
            SUM((ccip_send_requested_daily.fee_amount * tud.usd_amount)) as usd_amount,
            ccip_send_requested_daily.token as token
        FROM 
            {{ref('chainlink_arbitrum_ccip_send_requested_daily')}} ccip_send_requested_daily
        LEFT JOIN token_usd_daily tud ON tud.date_start = ccip_send_requested_daily.date_start AND tud.symbol = ccip_send_requested_daily.token
        {% if is_incremental() %}
            WHERE ccip_send_requested_daily.date_start >= date_trunc('day', now() - interval '{{incremental_interval}}' day)
        {% endif %}    
        GROUP BY 1, 5
    )
    
SELECT
    'arbitrum' as blockchain,
    date_start,
    date_month,
    token_amount,
    usd_amount,
    token
FROM 
    ccip_reward_daily
ORDER BY
    2, 6
