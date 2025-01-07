{{ config(
    schema = 'dex'
    , alias = 'ci_trades_test'
    , materialized = 'view'
    , tags = ['prod_exclude']
    )
}}

-- This view is only for CI testing purpose
-- It directly processes modified base_trades models and enriches them with token information

{% set modified_models = get_modified_base_trades() %}

WITH base_union AS (
    {% for file in modified_models %}
    {% set model_name = file.split('/')[-1].replace('.sql', '') %}
    SELECT *
    FROM {{ source('dex', model_name) }}
    WHERE block_date = current_date - interval '1' day
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

{{
    enrich_dex_trades(
        base_trades = 'base_union'
        , tokens_erc20_model = source('tokens', 'erc20')
        , filter = "1=1"
    )
}}
