{% macro add_amount_usd(
    trades_cte
    , prices_model
    , bought_amount_column
    , sold_amount_column
) %}

WITH trusted_tokens AS (
    SELECT
        contract_address
        , blockchain
    FROM {{ ref('prices_trusted_tokens') }}
),
prices AS (
    SELECT
        blockchain
        , contract_address
        , minute
        , price
    FROM {{ prices_model }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('minute') }}
    {% endif %}
),
enriched_trades AS (
    SELECT
        bt.*
        , CASE
            WHEN bt.token_bought_address IN (SELECT contract_address FROM trusted_tokens WHERE blockchain = bt.blockchain)
                THEN bt."{{ bought_amount_column }}" * pb.price
            WHEN bt.token_sold_address IN (SELECT contract_address FROM trusted_tokens WHERE blockchain = bt.blockchain)
                THEN bt."{{ sold_amount_column }}" * ps.price
            ELSE
                coalesce(
                    bt."{{ bought_amount_column }}" * pb.price
                    , bt."{{ sold_amount_column }}" * ps.price
                )
        END AS amount_usd
    FROM
        {{ trades_cte }} bt
    LEFT JOIN prices pb ON bt.token_bought_address = pb.contract_address
        AND bt.blockchain = pb.blockchain
        AND pb.minute = date_trunc('minute', bt.block_time)
    LEFT JOIN prices ps ON bt.token_sold_address = ps.contract_address
        AND bt.blockchain = ps.blockchain
        AND ps.minute = date_trunc('minute', bt.block_time)
)

SELECT * FROM enriched_trades

{% endmacro %}
