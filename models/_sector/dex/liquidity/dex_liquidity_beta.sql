{{ config(
    schema = 'dex'
    , alias = 'liquidity_beta'
    , materialized = 'view'
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
            blockchain
            , project
            , version
            , pool
            , token_address
            , balance 
            , balance_raw
            , balance_usd 
            , type 
            , fee
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