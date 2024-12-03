{{ config(
    schema = 'balancer',
    alias = 'static_token_prices',
    post_hook='{{ expose_spells(blockchains = \'["ethereum", "gnosis"]\',
                                spell_type = "project",
                                spell_name = "balancer",
                                contributors = \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v3_ethereum_static_tokens_prices'),
    ref('balancer_v3_gnosis_static_tokens_prices')
] %}

SELECT *
FROM (
    {% for model in balancer_models %}
    SELECT
        minute,
        blockchain,
        wrapped_token,
        underlying_token,
        static_atoken_symbol,
        underlying_token_symbol,
        decimals,
        median_price,
        next_change
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)