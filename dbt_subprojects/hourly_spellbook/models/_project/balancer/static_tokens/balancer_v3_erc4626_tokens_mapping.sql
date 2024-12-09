{{ config(
    schema = 'balancer_v3',
    alias = 'erc4626_token_mapping',
    post_hook='{{ expose_spells(blockchains = \'["ethereum", "gnosis"]\',
                                spell_type = "project",
                                spell_name = "balancer",
                                contributors = \'["viniabussafi"]\') }}'
    )
}}

{% set balancer_models = [
    ref('balancer_v3_ethereum_erc4626_tokens_mapping'),
    ref('balancer_v3_gnosis_erc4626_tokens_mapping')
] %}

SELECT *
FROM (
    {% for model in balancer_models %}
    SELECT
        blockchain,
        aToken,
        atoken_symbol,
        erc4626_token,
        erc4626_token_name,
        erc4626_token_symbol,
        underlying_token,
        underlying_token_symbol,
        underlying_token_decimals
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)