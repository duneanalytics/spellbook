{{ config(
    schema = 'balancer',
    
    alias = 'pools_fees',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon"]\',
                                "project",
                                "balancer",
                                \'["thetroyharris"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v2_arbitrum_pools_fees'),
    ref('balancer_v2_avalanche_c_pools_fees'),
    ref('balancer_v2_base_pools_fees'),
    ref('balancer_v2_ethereum_pools_fees'),
    ref('balancer_v2_gnosis_pools_fees'),
    ref('balancer_v2_optimism_pools_fees'),
    ref('balancer_v2_polygon_pools_fees')
] %}


SELECT *
FROM (
    {% for model in balancer_models %}
    SELECT
      blockchain
      , version
      , contract_address
      , tx_hash
      , index
      , tx_index
      , block_time
      , block_number
      , swap_fee_percentage
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)