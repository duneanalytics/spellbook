{{ config(
    schema = 'dex'
    , alias = 'pools_beta'
    , materialized = 'view'
    , unique_key = ['pool']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
    )
}}

{% set base_models = [
    ref('dex_ethereum_pools')
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
            , fee
            , tokens
            , tokens_in_pool
            , creation_block_time
            , creation_block_number
            , contract_address
        FROM 
            {{ base_model }}
        {% if is_incremental() %}
        WHERE
            {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)

SELECT * FROM base_union