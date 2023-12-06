{{ config(
    schema = 'balancer',
    alias = 'bpt_supply',
    post_hook = '{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon"]\',
                                "project",
                                "balancer",
                                \'["thetroyharris"]\') }}'
    )
}}

{% set bpt_supply_models = [
     ('arbitrum', ref('balancer_v2_arbitrum_bpt_supply'))
     , ('avalanche_c', ref('balancer_v2_avalanche_c_bpt_supply'))
     , ('base', ref('balancer_v2_base_bpt_supply'))
     , ('ethereum', ref('balancer_v2_ethereum_bpt_supply'))
     , ('gnosis', ref('balancer_v2_gnosis_bpt_supply'))
     , ('optimism', ref('balancer_v2_optimism_bpt_supply'))
     , ('polygon', ref('balancer_v2_polygon_bpt_supply'))
] %}

SELECT *
FROM (
    {% for bpt_supply_model in bpt_supply_models %}
    SELECT
    '{{ bpt_supply_model[0] }}' AS blockchain
    , block_time
    , block_number
    , lp_virtual_supply_raw
    , lp_virtual_supply
    , pool_id
    FROM {{ bpt_supply_model[1] }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
);
