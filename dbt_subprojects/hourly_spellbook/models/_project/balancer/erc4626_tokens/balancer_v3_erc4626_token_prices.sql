{{ config(
    schema = 'balancer_v3',
    alias = 'erc4626_token_prices',
    post_hook='{{ expose_spells(blockchains = \'["ethereum", "gnosis", "sonic", "arbitrum", "base"]\',
                                spell_type = "project",
                                spell_name = "balancer",
                                contributors = \'["viniabussafi"]\') }}'
    )
}}

    
{% set balancer_models = [
    ref('balancer_v3_ethereum_erc4626_token_prices'),
    ref('balancer_v3_gnosis_erc4626_token_prices'),
    ref('balancer_v3_sonic_erc4626_token_prices'),
    ref('balancer_v3_arbitrum_erc4626_token_prices'),
    ref('balancer_v3_base_erc4626_token_prices')
] %}

SELECT *
FROM (
    {% for model in balancer_models %}
    SELECT
        minute,
        blockchain,
        wrapped_token,
        underlying_token,
        erc4626_token_symbol,
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
