{{ config(
    schema = 'balancer',
    alias = 'bpt_prices',
    post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                            spell_type = "project",
                            spell_name = "balancer",
                            contributors = \'["thetroyharris", "viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v2_ethereum_bpt_prices'),
    ref('balancer_v2_arbitrum_bpt_prices'),
    ref('balancer_v2_polygon_bpt_prices'),
    ref('balancer_v2_gnosis_bpt_prices'),
    ref('balancer_v2_optimism_bpt_prices'),
    ref('balancer_v2_avalanche_c_bpt_prices'),
    ref('balancer_v2_base_bpt_prices'),
    ref('balancer_v2_zkevm_bpt_prices'),
    ref('balancer_v3_ethereum_bpt_prices'),
    ref('balancer_v3_gnosis_bpt_prices'),
    ref('balancer_v3_arbitrum_bpt_prices'),
    ref('balancer_v3_base_bpt_prices')      
] %}

SELECT *
FROM (
    {% for bpt_prices in balancer_models %}
    SELECT
        day,
        blockchain,
        version,
        decimals,
        contract_address,
        pool_type,
        bpt_price
    FROM {{ bpt_prices }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;
