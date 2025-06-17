{% macro transfers_enrich(
    base_transfers = null
    , tokens_erc20_model = null
    , prices_model = null
    , transfers_start_date = '2000-01-01'
    , blockchain = null
    , usd_amount_threshold = 25000000000
    )
%}

WITH base_transfers as (
    SELECT
        *
    FROM
        {{ base_transfers }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('block_date') }}
    {% else %}
    WHERE
        block_date >= TIMESTAMP '2025-06-10'
        AND block_date < TIMESTAMP '2025-06-17'
    {% endif %}
)
, prices AS (
    SELECT
        timestamp
        , blockchain
        , contract_address
        , decimals
        , symbol
        , price
    FROM
        {{ prices_model }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('timestamp') }}
    {% else %}
    WHERE
        timestamp >= TIMESTAMP '2025-06-10'
        AND timestamp < TIMESTAMP '2025-06-17'
    {% endif %}
)
, transfers as (
    SELECT
        t.unique_key
        , t.blockchain
        , t.block_month
        , t.block_date
        , t.block_time
        , t.block_number
        , t.tx_hash
        , t.evt_index
        , t.trace_address
        , t.token_standard
        , t.tx_from
        , t.tx_to
        , t.tx_index
        , t."from"
        , t.to
        , t.contract_address
        , coalesce(tokens_erc20.symbol, prices.symbol) AS symbol
        , t.amount_raw
        , t.amount_raw / power(10, coalesce(tokens_erc20.decimals, prices.decimals)) AS amount
        , prices.price AS price_usd
        , t.amount_raw / power(10, coalesce(tokens_erc20.decimals, prices.decimals)) * prices.price AS amount_usd
    FROM
        base_transfers as t
    LEFT JOIN
        {{ tokens_erc20_model }} as tokens_erc20
        ON tokens_erc20.blockchain = t.blockchain
        AND tokens_erc20.contract_address = t.contract_address
    LEFT JOIN
        prices
        ON date_trunc('hour', t.block_time) = prices.timestamp
        AND t.blockchain = prices.blockchain
        AND t.contract_address = prices.contract_address
)
, final as (
    SELECT
        unique_key
        , blockchain
        , block_month
        , block_date
        , block_time
        , block_number
        , tx_hash
        , evt_index
        , trace_address
        , token_standard
        , tx_from
        , tx_to
        , tx_index
        , "from"
        , to
        , contract_address
        , symbol
        , amount_raw
        , amount
        , price_usd
        , amount_usd
        /*
        , CASE
            WHEN amount_usd >= {{ usd_amount_threshold }}
                THEN CAST(NULL as double)
                ELSE amount_usd -- Select only transfers where USD amount is less than the threshold
            END AS amount_usd
        */
    FROM
        transfers
)
SELECT
    src.*
FROM
    final as src
{% if is_incremental() -%}
LEFT JOIN
    {{ this }} as tgt
    ON src.unique_key = tgt.unique_key
    AND src.block_month = tgt.block_month
    AND src.block_date = tgt.block_date
WHERE
    tgt.unique_key IS NULL
{% endif -%}
{%- endmacro %}