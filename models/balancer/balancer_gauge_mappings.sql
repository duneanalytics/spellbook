{{ config(
    schema = 'balancer',
    
    alias = 'gauge_mappings',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "balancer",
                                \'["msilb7"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_optimism_gauge_mappings')
] %}


SELECT *
FROM (
    {% for gauge_model in balancer_models %}
    SELECT
        blockchain
        , 'balancer' as project
        , version
        , pool_contract
        , pool_id
        , incentives_contract
        , incentives_type
        , evt_block_time
        , evt_block_number
        , contract_address
        , evt_tx_hash
        , evt_index
    FROM {{ gauge_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
