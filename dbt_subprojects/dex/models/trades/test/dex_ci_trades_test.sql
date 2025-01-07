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
{% set git_schema = 'git_dunesql_' ~ env_var('GIT_SHA', '') %}

WITH base_trades AS (
    {% for file in modified_models %}
    {% set model_name = file.split('/')[-1].replace('.sql', '') %}
    SELECT
        *,
        evt_tx_hash as tx_from,  -- For testing only, not used in production
        evt_tx_hash as tx_to     -- For testing only, not used in production
    FROM delta_prod.test_schema.{{ git_schema }}_{{ model_name }}
    WHERE block_date = current_date - interval '1' day
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

SELECT * FROM (
    {{
        enrich_dex_trades(
            base_trades = 'base_trades'
            , tokens_erc20_model = source('tokens', 'erc20')
            , filter = "1=1"
        )
    }}
)
