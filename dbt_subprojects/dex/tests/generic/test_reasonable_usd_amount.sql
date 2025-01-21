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

enrichments AS (
    SELECT
        bt.blockchain,
        bt.project,
        bt.version,
        bt.block_time,
        bt.tx_hash,
        bt.evt_index,
        CASE
            WHEN bt.project = 'curve' AND curve_optimism.pool_type is not null
                THEN bt.token_bought_amount_raw / power(10, CASE WHEN curve_optimism.pool_type = 'meta' AND curve_optimism.bought_id = INT256 '0' THEN 18 ELSE erc20_bought.decimals END)
            WHEN bt.project = 'curve'
                THEN bt.token_bought_amount_raw / power(10, erc20_bought.decimals)
            ELSE bt.token_bought_amount_raw / power(10, COALESCE(erc20_bought.decimals, 18))
        END AS token_bought_amount,
        CASE
            WHEN bt.project = 'curve' AND curve_ethereum.swap_type is not null
                THEN bt.token_sold_amount_raw / power(10, CASE WHEN curve_ethereum.swap_type = 'underlying_exchange_base' THEN 18 ELSE erc20_sold.decimals END)
            WHEN bt.project = 'curve' AND curve_optimism.pool_type is not null
                THEN bt.token_sold_amount_raw / power(10, CASE WHEN curve_optimism.pool_type = 'meta' AND curve_optimism.bought_id = INT256 '0' THEN erc20_bought.decimals ELSE erc20_sold.decimals END)
            WHEN bt.project = 'curve'
                THEN bt.token_sold_amount_raw / power(10, erc20_sold.decimals)
            ELSE bt.token_sold_amount_raw / power(10, COALESCE(erc20_sold.decimals, 18))
        END AS token_sold_amount,
        bt.token_bought_address,
        bt.token_sold_address
    FROM base_trades bt
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_bought
        ON erc20_bought.contract_address = bt.token_bought_address
        AND erc20_bought.blockchain = bt.blockchain
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sold
        ON erc20_sold.contract_address = bt.token_sold_address
        AND erc20_sold.blockchain = bt.blockchain
    LEFT JOIN {{ ref('curve_ethereum_base_trades') }} curve_ethereum
        ON curve_ethereum.tx_hash = bt.tx_hash
        AND curve_ethereum.evt_index = bt.evt_index
        AND bt.project = 'curve'
    LEFT JOIN {{ ref('curve_optimism_base_trades') }} curve_optimism
        ON curve_optimism.tx_hash = bt.tx_hash
        AND curve_optimism.evt_index = bt.evt_index
        AND bt.project = 'curve'
),

prices AS (
    SELECT
        blockchain,
        contract_address,
        minute,
        price
    FROM {{ source('prices', 'usd') }}
    WHERE minute >= NOW() - INTERVAL '{{ days_back }}' day  -- Match base_trades time filter
),

trusted_tokens AS (
    SELECT 
        contract_address,
        blockchain
    FROM {{ source('prices', 'trusted_tokens') }}
),

trades_with_prices AS (
    SELECT
        bt.*,
        COALESCE(
            CASE WHEN tt_bought.contract_address IS NOT NULL THEN bt.token_bought_amount * pb.price END,
            CASE WHEN tt_sold.contract_address IS NOT NULL THEN bt.token_sold_amount * ps.price END,
            bt.token_bought_amount * pb.price,
            bt.token_sold_amount * ps.price
        ) AS amount_usd
    FROM enrichments bt
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
