{{ config(
    schema = 'balancer',
    alias = 'protocol_revenue',
    
    post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "polygon", "gnosis", "optimism","avalanche_c", "base"]\',
                            "project",
                            "balancer",
                            \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v2_arbitrum_protocol_revenue'),
    ref('balancer_v2_avalanche_c_protocol_revenue'),
    ref('balancer_v2_base_protocol_revenue'),
    ref('balancer_v2_ethereum_protocol_revenue'),
    ref('balancer_v2_gnosis_protocol_revenue'),
    ref('balancer_v2_optimism_protocol_revenue'),
    ref('balancer_v2_polygon_protocol_revenue'),
] %}

SELECT *
FROM (
    {% for protocol_revenue in balancer_models %}
    SELECT
        day,
        pool_id,
        pool_address,
        pool_symbol,
        blockchain,
        token_address,
        token_amount_raw,
        token_amount,
        protocol_fee_collected_usd, 
        treasury_share,
        treasury_revenue_usd
    FROM {{ protocol_revenue }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
