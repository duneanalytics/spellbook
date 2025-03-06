{{ config(
    schema = 'balancer_v3',
    alias = 'lbps',
    post_hook='{{ expose_spells(blockchains = \'["arbitrum", "base", "ethereum", "gnosis"]\',
                                spell_type = "project",
                                spell_name = "balancer_v3",
                                contributors = \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v3_arbitrum_lbps'),
    ref('balancer_v3_base_lbps'),
    ref('balancer_v3_ethereum_lbps'),
    ref('balancer_v3_gnosis_lbps')
] %}


SELECT *
FROM (
    {% for model in balancer_models %}
    SELECT
        blockchain,
        pool_symbol,
        pool_address,
        start_time,
        end_time,
        project_token,
        project_token_symbol,
        reserve_token,
        reserve_token_symbol,
        project_token_start_weight,
        reserve_token_start_weight,
        project_token_end_weight,
        reserve_token_end_weight
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
