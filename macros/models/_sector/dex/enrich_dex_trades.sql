{% macro enrich_dex_trades(
    stg_trades = null
    , tokens_erc20_model = null
    , prices_model = null
    )
%}

WITH stg as (
    SELECT
        *
    FROM
        {{ stg_trades }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('block_time') }}
    {% endif %}
)
, prices AS (
    SELECT
        blockchain
        , contract_address
        , minute
        , price
    FROM
        {{ prices_model }}
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('minute') }}
    {% endif %}
)
, enrichments AS (
    SELECT
        stg.blockchain
        , stg.project
        , stg.version
        , stg.block_month
        , stg.block_date
        , stg.block_time
        , stg.block_number
        , erc20_bought.symbol AS token_bought_symbol
        , erc20_sold.symbol AS token_sold_symbol
        , case
            when lower(erc20_bought.symbol) > lower(erc20_sold.symbol) then concat(erc20_sold.symbol, '-', erc20_bought.symbol)
            else concat(erc20_bought.symbol, '-', erc20_sold.symbol)
            end AS token_pair
        , stg.token_bought_amount_raw / power(10, erc20_bought.decimals) AS token_bought_amount
        , stg.token_sold_amount_raw / power(10, erc20_sold.decimals) AS token_sold_amount
        , stg.token_bought_amount_raw
        , stg.token_sold_amount_raw
        , coalesce(
                stg.token_bought_amount_raw / power(10, erc20_bought.decimals) * p_bought.price,
                stg.token_sold_amount_raw / power(10, erc20_sold.decimals) * p_sold.price
            ) AS amount_usd
        , stg.token_bought_address
        , stg.token_sold_address
        , coalesce(stg.taker, stg.tx_from) AS taker
        , stg.maker
        , stg.project_contract_address
        , stg.tx_hash
        , stg.tx_from
        , stg.tx_to
        , stg.evt_index
    FROM
        stg as stg
    LEFT JOIN
        {{ tokens_erc20_model }} as erc20_bought
        ON erc20_bought.contract_address = stg.token_bought_address
        AND erc20_bought.blockchain = stg.blockchain
    LEFT JOIN
        {{ tokens_erc20_model }} as erc20_sold
        ON erc20_sold.contract_address = stg.token_sold_address
        AND erc20_sold.blockchain = stg.blockchain
    LEFT JOIN
        prices as p_bought
        ON p_bought.minute = date_trunc('minute', stg.block_time)
        AND p_bought.contract_address = stg.token_bought_address
        AND p_bought.blockchain = stg.blockchain
    LEFT JOIN
        prices as p_sold
        ON p_sold.minute = date_trunc('minute', stg.block_time)
        AND p_sold.contract_address = stg.token_sold_address
        AND p_sold.blockchain = stg.blockchain
)

select
    *
from
    enrichments
{% endmacro %}