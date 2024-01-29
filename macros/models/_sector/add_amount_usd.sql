{% macro add_amount_usd(
    trades_cte
    , prices_model
    , bought_amount_column
    , sold_amount_column
) %}

SELECT
    bt.*
    , CASE
        WHEN bt.token_bought_address IN (
                SELECT contract_address
                FROM {{ ref('prices_trusted_tokens') }}
                WHERE blockchain = bt.blockchain
            )
            THEN bt."{{ bought_amount_column }}" * pb.price
        WHEN bt.token_sold_address IN (
                SELECT contract_address
                FROM {{ ref('prices_trusted_tokens') }}
                WHERE blockchain = bt.blockchain
            )
            THEN bt."{{ sold_amount_column }}" * ps.price
        ELSE
            coalesce(
                bt."{{ bought_amount_column }}" * pb.price,
                bt."{{ sold_amount_column }}" * ps.price
            )
    END AS amount_usd
FROM
    {{ trades_cte }} bt
LEFT JOIN (
        SELECT blockchain
            , contract_address
            , minute
            , price
        FROM {{ prices_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('minute') }}
        {% endif %}
    ) pb
    ON bt.token_bought_address = pb.contract_address
    AND bt.blockchain = pb.blockchain
    AND pb.minute = date_trunc('minute', bt.block_time)
LEFT JOIN (
        SELECT blockchain
            , contract_address
            , minute
            , price
        FROM {{ prices_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('minute') }}
        {% endif %}
    ) ps
    ON bt.token_sold_address = ps.contract_address
    AND bt.blockchain = ps.blockchain
    AND ps.minute = date_trunc('minute', bt.block_time)

{% endmacro %}
