{% test test_acceptable_usd_amount(model, column_name, max_value=1000000000, days_back=30) %}

{# 
    Test to ensure USD amounts are within a reasonable range
    Args:
        model: The model to test
        column_name: The column containing USD amounts
        max_value: Maximum allowed USD amount (default: $1B)
        days_back: Number of days to look back (default: 30)
#}

WITH base_trades AS (
    SELECT *
    FROM {{ model }}
    WHERE block_time >= NOW() - INTERVAL '{{ days_back }}' day  -- Check trades for the specified days
),

-- Split trades by project for optimized processing
curve_trades AS (
    SELECT *
    FROM base_trades
    WHERE project = 'curve'
),

balancer_v3_trades AS (
    SELECT *
    FROM base_trades
    WHERE project = 'balancer' AND version = '3'
),

other_trades AS (
    SELECT *
    FROM base_trades
    WHERE project != 'curve' 
    AND NOT (project = 'balancer' AND version = '3')
),

-- Process Curve trades
curve_enriched AS (
    SELECT
        bt.blockchain,
        bt.project,
        bt.version,
        bt.block_time,
        bt.tx_hash,
        bt.evt_index,
        CASE
            WHEN curve_optimism.pool_type is not null
                THEN bt.token_bought_amount_raw / power(10, CASE WHEN curve_optimism.pool_type = 'meta' AND curve_optimism.bought_id = INT256 '0' THEN 18 ELSE erc20_bought.decimals END)
            ELSE bt.token_bought_amount_raw / power(10, erc20_bought.decimals)
        END AS token_bought_amount,
        CASE
            WHEN curve_ethereum.swap_type is not null
                THEN bt.token_sold_amount_raw / power(10, CASE WHEN curve_ethereum.swap_type = 'underlying_exchange_base' THEN 18 ELSE erc20_sold.decimals END)
            WHEN curve_optimism.pool_type is not null
                THEN bt.token_sold_amount_raw / power(10, CASE WHEN curve_optimism.pool_type = 'meta' AND curve_optimism.bought_id = INT256 '0' THEN erc20_bought.decimals ELSE erc20_sold.decimals END)
            ELSE bt.token_sold_amount_raw / power(10, erc20_sold.decimals)
        END AS token_sold_amount,
        bt.token_bought_address,
        bt.token_sold_address
    FROM curve_trades bt
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_bought
        ON erc20_bought.contract_address = bt.token_bought_address
        AND erc20_bought.blockchain = bt.blockchain
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sold
        ON erc20_sold.contract_address = bt.token_sold_address
        AND erc20_sold.blockchain = bt.blockchain
    LEFT JOIN {{ ref('curve_ethereum_base_trades') }} curve_ethereum
        ON curve_ethereum.tx_hash = bt.tx_hash
        AND curve_ethereum.evt_index = bt.evt_index
    LEFT JOIN {{ ref('curve_optimism_base_trades') }} curve_optimism
        ON curve_optimism.tx_hash = bt.tx_hash
        AND curve_optimism.evt_index = bt.evt_index
),

-- Process Balancer V3 trades
balancer_v3_enriched AS (
    SELECT
        bt.blockchain,
        bt.project,
        bt.version,
        bt.block_time,
        bt.tx_hash,
        bt.evt_index,
        bt.token_bought_amount_raw / power(10, COALESCE(erc20_bought.decimals, 18)) AS token_bought_amount,
        bt.token_sold_amount_raw / power(10, COALESCE(erc20_sold.decimals, 18)) AS token_sold_amount,
        bt.token_bought_address,
        bt.token_sold_address
    FROM balancer_v3_trades bt
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_bought
        ON erc20_bought.contract_address = bt.token_bought_address
        AND erc20_bought.blockchain = bt.blockchain
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sold
        ON erc20_sold.contract_address = bt.token_sold_address
        AND erc20_sold.blockchain = bt.blockchain
),

-- Process other trades
other_enriched AS (
    SELECT
        bt.blockchain,
        bt.project,
        bt.version,
        bt.block_time,
        bt.tx_hash,
        bt.evt_index,
        bt.token_bought_amount_raw / power(10, COALESCE(erc20_bought.decimals, 18)) AS token_bought_amount,
        bt.token_sold_amount_raw / power(10, COALESCE(erc20_sold.decimals, 18)) AS token_sold_amount,
        bt.token_bought_address,
        bt.token_sold_address
    FROM other_trades bt
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_bought
        ON erc20_bought.contract_address = bt.token_bought_address
        AND erc20_bought.blockchain = bt.blockchain
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sold
        ON erc20_sold.contract_address = bt.token_sold_address
        AND erc20_sold.blockchain = bt.blockchain
),

prices AS (
    SELECT
        blockchain,
        contract_address,
        minute,
        price
    FROM {{ source('prices', 'usd') }}
    WHERE minute >= NOW() - INTERVAL '{{ days_back }}' day
),

trusted_tokens AS (
    SELECT 
        contract_address,
        blockchain
    FROM {{ source('prices', 'trusted_tokens') }}
),

erc4626_prices AS (
    SELECT
        minute,
        blockchain,
        wrapped_token,
        decimals,
        APPROX_PERCENTILE(median_price, 0.5) AS price,
        LEAD(minute, 1, NOW()) OVER (PARTITION BY wrapped_token ORDER BY minute) AS time_of_next_change
    FROM {{ source('balancer_v3', 'erc4626_token_prices') }}
    WHERE minute >= NOW() - INTERVAL '{{ days_back }}' day
    GROUP BY 1, 2, 3, 4
),

-- Calculate USD amounts for regular trades (Curve and Others)
regular_trades_with_prices AS (
    SELECT
        bt.*,
        COALESCE(
            CASE WHEN tt_bought.contract_address IS NOT NULL THEN bt.token_bought_amount * pb.price END,
            CASE WHEN tt_sold.contract_address IS NOT NULL THEN bt.token_sold_amount * ps.price END,
            bt.token_bought_amount * pb.price,
            bt.token_sold_amount * ps.price
        ) AS amount_usd
    FROM (
        SELECT * FROM curve_enriched
        UNION ALL
        SELECT * FROM other_enriched
    ) bt
    LEFT JOIN prices pb
        ON bt.token_bought_address = pb.contract_address
        AND bt.blockchain = pb.blockchain
        AND pb.minute = date_trunc('minute', bt.block_time)
    LEFT JOIN prices ps
        ON bt.token_sold_address = ps.contract_address
        AND bt.blockchain = ps.blockchain
        AND ps.minute = date_trunc('minute', bt.block_time)
    LEFT JOIN trusted_tokens tt_bought
        ON bt.token_bought_address = tt_bought.contract_address
        AND bt.blockchain = tt_bought.blockchain
    LEFT JOIN trusted_tokens tt_sold
        ON bt.token_sold_address = tt_sold.contract_address
        AND bt.blockchain = tt_sold.blockchain
),

-- Calculate USD amounts for Balancer V3 trades
balancer_v3_with_prices AS (
    SELECT
        bt.*,
        COALESCE(
            -- Try ERC4626 prices first
            bt.token_bought_amount * erc4626a.price,
            bt.token_sold_amount * erc4626b.price,
            -- Fall back to regular prices if ERC4626 prices not available
            CASE WHEN tt_bought.contract_address IS NOT NULL THEN bt.token_bought_amount * pb.price END,
            CASE WHEN tt_sold.contract_address IS NOT NULL THEN bt.token_sold_amount * ps.price END,
            bt.token_bought_amount * pb.price,
            bt.token_sold_amount * ps.price
        ) AS amount_usd
    FROM balancer_v3_enriched bt
    LEFT JOIN erc4626_prices erc4626a
        ON erc4626a.wrapped_token = bt.token_bought_address
        AND erc4626a.minute <= bt.block_time
        AND bt.block_time < erc4626a.time_of_next_change
        AND bt.blockchain = erc4626a.blockchain
    LEFT JOIN erc4626_prices erc4626b
        ON erc4626b.wrapped_token = bt.token_sold_address
        AND erc4626b.minute <= bt.block_time
        AND bt.block_time < erc4626b.time_of_next_change   
        AND bt.blockchain = erc4626b.blockchain
    -- Fallback to regular prices
    LEFT JOIN prices pb
        ON bt.token_bought_address = pb.contract_address
        AND bt.blockchain = pb.blockchain
        AND pb.minute = date_trunc('minute', bt.block_time)
    LEFT JOIN prices ps
        ON bt.token_sold_address = ps.contract_address
        AND bt.blockchain = ps.blockchain
        AND ps.minute = date_trunc('minute', bt.block_time)
    LEFT JOIN trusted_tokens tt_bought
        ON bt.token_bought_address = tt_bought.contract_address
        AND bt.blockchain = tt_bought.blockchain
    LEFT JOIN trusted_tokens tt_sold
        ON bt.token_sold_address = tt_sold.contract_address
        AND bt.blockchain = tt_sold.blockchain
),

-- Combine all trades with their USD amounts
all_trades_with_prices AS (
    SELECT * FROM regular_trades_with_prices
    UNION ALL
    SELECT * FROM balancer_v3_with_prices
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
    FROM all_trades_with_prices
    WHERE amount_usd > {{ max_value }}
    AND amount_usd is not null
)

SELECT *
FROM validation

{% endtest %}
