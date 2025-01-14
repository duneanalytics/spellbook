{% macro test_reasonable_usd_amount(model, column_name, max_value=1000000000) %}

{# 
    Test to ensure USD amounts are within a reasonable range
    Args:
        model: The model to test
        column_name: The column containing USD amounts
        max_value: Maximum allowed USD amount (default: $1B)
#}

WITH base_trades AS (
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
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        evt_index
    FROM {{ model }}
    WHERE block_time >= NOW() - INTERVAL '1' day  -- Only check recent trades
),

enriched_trades AS (
    {{
        enrich_dex_trades(
            base_trades = 'base_trades'
            , tokens_erc20_model = source('tokens', 'erc20')
            , filter = "1=1"
        )
    }}
),

validation AS (
    SELECT
        blockchain,
        project,
        version,
        tx_hash,
        evt_index,
        block_time,
        amount_usd
    FROM enriched_trades
    WHERE amount_usd > {{ max_value }}
    AND amount_usd is not null
)

SELECT *
FROM validation

{% endmacro %}
