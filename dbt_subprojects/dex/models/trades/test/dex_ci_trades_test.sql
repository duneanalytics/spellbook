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
        blockchain,
        project,
        version,
        block_month,
        block_date,
        block_time,
        block_number,
        token_bought_amount_raw,
        token_sold_amount_raw,
        CAST(token_bought_address AS varbinary) as token_bought_address,
        CAST(token_sold_address AS varbinary) as token_sold_address,
        CAST(taker AS varbinary) as taker,
        CAST(maker AS varbinary) as maker,
        CAST(project_contract_address AS varbinary) as project_contract_address,
        tx_hash,
        evt_index,
        cast(null as varbinary) as tx_from,  -- For testing only, not used in production
        cast(null as varbinary) as tx_to     -- For testing only, not used in production
    FROM delta_prod.test_schema.{{ git_schema }}_{{ model_name }}
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
