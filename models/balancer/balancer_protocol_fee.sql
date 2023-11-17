{{ config(
    schema = 'balancer',
    alias = 'protocol_fee',
    
    post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "polygon", "gnosis", "optimism","avalanche_c", "base"]\',
                            "project",
                            "balancer",
                            \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v2_arbitrum_protocol_fee'),
    ref('balancer_v2_avalanche_c_protocol_fee'),
    ref('balancer_v2_base_protocol_fee'),
    ref('balancer_v2_ethereum_protocol_fee'),
    ref('balancer_v2_gnosis_protocol_fee'),
    ref('balancer_v2_optimism_protocol_fee'),
    ref('balancer_v2_polygon_protocol_fee'),
] %}

SELECT *
FROM (
    {% for protocol_fee in balancer_models %}
    SELECT
        day,
        pool_id,
        pool_address,
        pool_symbol,
        blockchain,
       -- token_address,
        token_amount_raw,
        token_amount,
        protocol_fee_collected_usd, 
        treasury_share,
        treasury_revenue_usd
    FROM {{ protocol_fee }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
