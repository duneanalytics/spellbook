{{ config(
    schema = 'dex'
    , alias = 'ci_trades_test'
    , materialized = 'view'
    , tags = ['prod_exclude']
    )
}}

-- This view is only for CI testing purpose
-- It directly processes modified base_trades models and enriches them with token information

{% set modified_base_trades = dbt_utils.get_modified_models('base_trades') %}

WITH base_union AS (
    {% for model in modified_base_trades %}
    SELECT *
    FROM {{ model }}
    WHERE block_date = current_date - interval '1' day
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

{{
    enrich_dex_trades(
        base_trades = 'base_union'
        , filter = "1=1"  -- Date filter already applied in base_union
        , tokens_erc20_model = source('tokens', 'erc20')
    )
}}
