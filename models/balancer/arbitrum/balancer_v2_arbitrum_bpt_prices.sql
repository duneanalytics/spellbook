{{
    config(
        schema = 'balancer_v2_arbitrum',
        alias = 'bpt_prices',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'hour','contract_address'],
        post_hook = '{{ expose_spells(\'["arbitrum"]\',
                                    "project",
                                    "balancer_v2",
                                    \'["victorstefenon", "thetroyharris", "viniabussafi"]\') }}'
    )
}}

WITH
    bpt_supply AS (
        SELECT 
            s.day,
            s.blockchain,
            s.token_address,
            s.supply
        FROM {{ ref ('balancer_v2_arbitrum_bpt_supply') }} s
        {% if is_incremental() %}
        WHERE s.day >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    ),

    liquidity AS (
        SELECT 
            l.day,
            l.blockchain,
            l.pool_address,
            s.protocol_liquidity_usd AS pool_liquidity
        FROM {{ ref ('balancer_v2_arbitrum_liquidity') }} l
        {% if is_incremental() %}
        WHERE l.day >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    )

SELECT
    l.blockchain,
    l.day,
    s.contract_address,
    l.pool_liquidity / s.supply AS bpt_price
FROM supply s
LEFT JOIN liquidity l ON l.day = s.day AND l.blockchain = s.blockchain AND l.pool_Address = s.token_address
ORDER BY 2 DESC, 3
