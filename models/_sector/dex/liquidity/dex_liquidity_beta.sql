{{ config(
    schema = 'dex'
    , alias = 'liquidity_beta'
    , materialized = 'incremental'
    , unique_key = ['pool', 'token_address', 'day']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
    )
}}

{% set base_models = [
    ref('dex_ethereum_liquidity')
] %}

WITH base_union AS (
    SELECT *
    FROM (
        {% for base_model in base_models %}
        SELECT
            day
            , blockchain
            , project
            , version
            , pool
            , token_address
            , token_symbol
            , balance
            , balance_usd 
        FROM 
            {{ base_model }}
        {% if is_incremental() %}
        WHERE
            {{ incremental_predicate('day') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

SELECT * FROM base_union