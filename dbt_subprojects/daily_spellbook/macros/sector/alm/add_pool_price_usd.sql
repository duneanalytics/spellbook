{% macro add_pool_price_usd(
    pool_prices_cte
) %}

-- This macro adds the price_usd column to the pool_prices_cte
-- Required columns in pool_prices_cte:
    --  blockchain
    --  , token0_address
    --  , token1_address
    --  , price
    --  , block_time

WITH trusted_tokens AS (
    SELECT contract_address
         , blockchain
    FROM {{ source('prices','trusted_tokens') }}
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
    pp.*
    , COALESCE(
        CASE WHEN tt0.contract_address IS NOT NULL THEN pp.price / p0.price END,
        CASE WHEN tt1.contract_address IS NOT NULL THEN p1.price * pp.price END,
        pp.price / p0.price,
        p1.price * pp.price
    ) AS price_usd
FROM
    {{ pool_prices_cte }} pp
LEFT JOIN prices p0
    ON pp.token0_address = p0.contract_address
    AND pp.blockchain = p0.blockchain
    AND p0.minute = date_trunc('minute', pp.block_time)
LEFT JOIN prices p1
    ON pp.token1_address = p1.contract_address
    AND pp.blockchain = p1.blockchain
    AND p1.minute = date_trunc('minute', pp.block_time)
LEFT JOIN trusted_tokens tt0
    ON pp.token0_address = tt0.contract_address
    AND pp.blockchain = tt0.blockchain
LEFT JOIN trusted_tokens tt1
    ON pp.token1_address = tt1.contract_address
    AND pp.blockchain = tt1.blockchain

{% endmacro %}
