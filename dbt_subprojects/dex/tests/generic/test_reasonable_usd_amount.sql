{% test test_reasonable_usd_amount(model, column_name, max_value=1000000000) %}

{# 
    Test to ensure USD amounts are within a reasonable range
    Args:
        model: The model to test
        column_name: The column containing USD amounts
        max_value: Maximum allowed USD amount (default: $1B)
#}

WITH base_trades AS (
    SELECT
        *
    FROM {{ model }}
    WHERE block_time >= NOW() - INTERVAL '1' day  -- Only check recent trades
),

trades_with_prices AS (
    {{
        add_amount_usd(
            trades_cte = 'base_trades'
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
    FROM trades_with_prices
    WHERE amount_usd > {{ max_value }}
    AND amount_usd is not null
)

SELECT *
FROM validation

{% endtest %}
