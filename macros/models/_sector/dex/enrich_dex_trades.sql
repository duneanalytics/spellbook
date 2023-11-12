{% macro enrich_dex_trades(
    base_trades = null
    , tokens_erc20_model = null
    , prices_model = null
    )
%}

WITH base_trades as (
    SELECT
        *
    FROM
        {{ base_trades }}
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
        base_trades.blockchain
        , base_trades.project
        , base_trades.version
        , base_trades.block_month
        , base_trades.block_date
        , base_trades.block_time
        , base_trades.block_number
        , erc20_bought.symbol AS token_bought_symbol
        , erc20_sold.symbol AS token_sold_symbol
        , case
            when lower(erc20_bought.symbol) > lower(erc20_sold.symbol) then concat(erc20_sold.symbol, '-', erc20_bought.symbol)
            else concat(erc20_bought.symbol, '-', erc20_sold.symbol)
            end AS token_pair
        , base_trades.token_bought_amount_raw / power(10, erc20_bought.decimals) AS token_bought_amount
        , base_trades.token_sold_amount_raw / power(10, erc20_sold.decimals) AS token_sold_amount
        , base_trades.token_bought_amount_raw
        , base_trades.token_sold_amount_raw
        , coalesce(
                base_trades.token_bought_amount_raw / power(10, erc20_bought.decimals) * p_bought.price,
                base_trades.token_sold_amount_raw / power(10, erc20_sold.decimals) * p_sold.price
            ) AS amount_usd
        , base_trades.token_bought_address
        , base_trades.token_sold_address
        , coalesce(base_trades.taker, base_trades.tx_from) AS taker
        , base_trades.maker
        , base_trades.project_contract_address
        , base_trades.tx_hash
        , base_trades.tx_from
        , base_trades.tx_to
        , base_trades.evt_index
    FROM
        base_trades
    LEFT JOIN
        {{ tokens_erc20_model }} as erc20_bought
        ON erc20_bought.contract_address = base_trades.token_bought_address
        AND erc20_bought.blockchain = base_trades.blockchain
    LEFT JOIN
        {{ tokens_erc20_model }} as erc20_sold
        ON erc20_sold.contract_address = base_trades.token_sold_address
        AND erc20_sold.blockchain = base_trades.blockchain
    LEFT JOIN
        prices as p_bought
        ON p_bought.minute = date_trunc('minute', base_trades.block_time)
        AND p_bought.contract_address = base_trades.token_bought_address
        AND p_bought.blockchain = base_trades.blockchain
    LEFT JOIN
        prices as p_sold
        ON p_sold.minute = date_trunc('minute', base_trades.block_time)
        AND p_sold.contract_address = base_trades.token_sold_address
        AND p_sold.blockchain = base_trades.blockchain
)

select
    *
from
    enrichments
{% endmacro %}