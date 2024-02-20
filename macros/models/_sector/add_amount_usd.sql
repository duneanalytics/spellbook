{% macro add_amount_usd(
    trades_cte
) %}

-- This macro adds the amount_usd column to the trades_cte
-- Required columns in trades_cte:
    --  blockchain
    --  , token_bought_address
    --  , token_sold_address
    --  , token_bought_amount
    --  , token_sold_amount
    --  , block_time

WITH trusted_tokens AS (
    SELECT contract_address
         , blockchain
    FROM {{ ref('prices_trusted_tokens') }}
)
, prices AS (
    SELECT
        blockchain
        , contract_address
        , minute
        , price
    FROM
        {{ source('prices','usd') }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('minute') }}
    {% endif %}
)

SELECT
    bt.*
    , COALESCE(
        CASE WHEN tt_bought.contract_address IS NOT NULL THEN bt.token_bought_amount * pb.price END,
        CASE WHEN tt_sold.contract_address IS NOT NULL THEN bt.token_sold_amount * ps.price END,
        bt.token_bought_amount * pb.price,
        bt.token_sold_amount * ps.price
    ) AS amount_usd
FROM
    {{ trades_cte }} bt
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

{% endmacro %}
