{{ config(
    schema = 'dex'
    , alias = 'ci_trades_test'
    , materialized = 'view'
    , tags = ['prod_exclude']
    )
}}

-- This view is only for CI testing purpose
-- It directly processes modified base_trades models and enriches them with transaction and token information

{% set modified_base_trades = dbt_utils.get_modified_models('base_trades') %}

WITH base_union AS (
    {% for model in modified_base_trades %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_amount_raw
        , token_sold_amount_raw
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , evt_index
    FROM {{ model }}
    WHERE block_date = current_date - interval '1' day
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

, with_tx AS (
    {{
        add_tx_columns_dynamic(
            model_cte = 'base_union'
            , columns = ['from', 'to', 'index']
        )
    }}
)

{{
    enrich_dex_trades(
        base_trades = 'with_tx'
        , filter = "1=1"  -- Date filter already applied in base_union
        , tokens_erc20_model = source('tokens', 'erc20')
    )
}}
