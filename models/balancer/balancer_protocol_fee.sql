{{ config(
    schema = 'balancer',
    alias = 'protocol_fee',
    post_hook='{{ expose_spells(\'["ethereum", "arbitrum", "polygon", "gnosis", "optimism","avalanche_c", "base"]\',
                            "project",
                            "balancer",
                            \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_v2_models = [
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
    {% for protocol_fee in balancer_v2_models %}
    SELECT
        day,
        pool_id,
        pool_address,
        pool_symbol,
        '2' AS version,
        blockchain,
        pool_type,
        token_address,
        token_symbol,
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
