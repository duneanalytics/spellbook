{{ config(
        schema = 'tapio',
        alias = 'liquidity',
        post_hook='{{ expose_spells(blockchains = \'["base", "sonic"]\',
                                spell_type = "project",
                                spell_name = "tapio",
                                contributors = \'["brunota20"]\') }}'
    ) 
}}

{% set tapio_models = [
    ref('tapio_base_liquidity'),
    ref('tapio_sonic_liquidity')
] %}

SELECT * FROM (
    {% for liquidity_model in tapio_models %}
    SELECT
        blockchain,
        project,
        version,
        day,
        pool_address,
        token_address,
        token_symbol,
        token_balance_raw,
        token_balance,
        token_balance_usd
    FROM {{ liquidity_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
);