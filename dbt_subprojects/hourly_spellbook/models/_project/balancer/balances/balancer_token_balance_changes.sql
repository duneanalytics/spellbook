{{ config(
    schema = 'balancer',
    alias = 'token_balance_changes',
    post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                                spell_type = "project",
                                spell_name = "balancer",
                                contributors = \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v2_arbitrum_token_balance_changes'),
    ref('balancer_v2_avalanche_c_token_balance_changes'),
    ref('balancer_v2_base_token_balance_changes'),
    ref('balancer_v2_ethereum_token_balance_changes'),
    ref('balancer_v2_gnosis_token_balance_changes'),
    ref('balancer_v2_optimism_token_balance_changes'),
    ref('balancer_v2_polygon_token_balance_changes'),
    ref('balancer_v2_zkevm_token_balance_changes'),
    ref('balancer_v3_ethereum_token_balance_changes'),
    ref('balancer_v3_gnosis_token_balance_changes'),
    ref('balancer_v3_arbitrum_token_balance_changes'),
    ref('balancer_v3_base_token_balance_changes')        
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
      , pool_id
      , pool_address
      , pool_symbol
      , pool_type 
      , version 
      , token_address
      , token_symbol
      , delta_amount_raw
      , delta_amount
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)