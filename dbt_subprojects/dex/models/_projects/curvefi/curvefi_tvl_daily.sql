{{ config(
        schema = 'curvefi',
        alias = 'tvl_daily'
        , post_hook='{{ hide_spells() }}'
        )
}}

{% set curvefi_models = [
ref('curvefi_ethereum_tvl_daily')
] %}


SELECT *
FROM (
    {% for dex_pool_model in curvefi_models %}
    SELECT
        block_month
        , block_date
        , blockchain
        , project
        , version
        , id
        , token0 
        , token0_symbol 
        , token0_balance 
        , token0_balance_usd
        , token1 
        , token1_symbol 
        , token1_balance 
        , token1_balance_usd 
        , token2 
        , token2_symbol 
        , token2_balance 
        , token2_balance_usd 
        , token3 
        , token3_symbol 
        , token3_balance 
        , token3_balance_usd
    FROM {{ dex_pool_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)