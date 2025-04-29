{{ config(
    schema = 'balancer',
    alias = 'protocol_fee',
    post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                            spell_type = "project",
                            spell_name = "balancer",
                            contributors = \'["viniabussafi"]\') }}'
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
    ref('balancer_v2_zkevm_protocol_fee'),
    ref('balancer_v3_ethereum_protocol_fee'),
    ref('balancer_v3_gnosis_protocol_fee'),
    ref('balancer_v3_arbitrum_protocol_fee'),
    ref('balancer_v3_base_protocol_fee')  
] %}

SELECT *
FROM (
    {% for protocol_fee in balancer_models %}
    SELECT
        day,
        pool_id,
        pool_address,
        pool_symbol,
        version,
        blockchain,
        pool_type,
        fee_type,
        token_address,
        token_symbol,
        token_amount_raw,
        token_amount,
        protocol_fee_collected_usd, 
        treasury_share,
        treasury_fee_usd,
        lp_fee_collected_usd
    FROM {{ protocol_fee }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
