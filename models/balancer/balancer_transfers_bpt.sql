{{ config(
    schema = 'balancer',
    alias = 'transfers_bpt',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon"]\',
                                "project",
                                "balancer",
                                \'["thetroyharris", "victorstefenon", "viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v2_arbitrum_transfers_bpt'),
    ref('balancer_v2_avalanche_c_transfers_bpt'),
    ref('balancer_v2_base_transfers_bpt'),
    ref('balancer_v2_ethereum_transfers_bpt'),
    ref('balancer_v2_gnosis_transfers_bpt'),
    ref('balancer_v2_optimism_transfers_bpt'),
    ref('balancer_v2_polygon_transfers_bpt')
] %}

SELECT *
FROM (
    {% for model in balancer_models %}
    SELECT
        blockchain
      , version    
      , contract_address
      , block_date
      , evt_tx_hash
      , evt_index
      , evt_block_time
      , evt_block_number
      , "from"
      , to
      , value
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)