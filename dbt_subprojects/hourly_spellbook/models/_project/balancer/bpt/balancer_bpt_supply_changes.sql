{{ config(
    schema = 'balancer',
    alias = 'bpt_supply_changes',
    post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                                spell_type = "project",
                                spell_name = "balancer",
                                contributors = \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v2_arbitrum_bpt_supply_changes'),
    ref('balancer_v2_avalanche_c_bpt_supply_changes'),
    ref('balancer_v2_base_bpt_supply_changes'),
    ref('balancer_v2_ethereum_bpt_supply_changes'),
    ref('balancer_v2_gnosis_bpt_supply_changes'),
    ref('balancer_v2_optimism_bpt_supply_changes'),
    ref('balancer_v2_polygon_bpt_supply_changes'),
    ref('balancer_v2_zkevm_bpt_supply_changes'),
    ref('balancer_v3_ethereum_bpt_supply_changes'),
    ref('balancer_v3_gnosis_bpt_supply_changes'),
    ref('balancer_v3_arbitrum_bpt_supply_changes'),
    ref('balancer_v3_base_bpt_supply_changes')    
] %}

SELECT *
FROM (
    {% for model in balancer_models %}
    SELECT
        block_date
      , evt_block_time
      , evt_block_number
      , blockchain
      , evt_tx_hash
      , evt_index
      , pool_type
      , pool_symbol   
      , version 
      , label
      , token_address
      , delta_amount_raw
      , delta_amount
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)