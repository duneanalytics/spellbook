{% macro enrich_dex_trades(
    base_trades = null,
    tokens_erc20_model=null,
    prices_model=null
    )
%}

WITH prices AS (
    SELECT
        blockchain,
        contract_address,
        minute,
        price
    FROM {{ prices_model }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('minute') }}
    {% endif %}
),

enrichments AS (
    SELECT
        base.blockchain,
        base.project,
        base.version,
        base.block_month,
        base.block_date,
        base.block_time,
        base.block_number,
        erc20_bought.symbol AS token_bought_symbol,
        erc20_sold.symbol AS token_sold_symbol,
        case
            when lower(erc20_bought.symbol) > lower(erc20_sold.symbol) then concat(erc20_sold.symbol, '-', erc20_bought.symbol)
            else concat(erc20_bought.symbol, '-', erc20_sold.symbol)
            end AS token_pair,
        base.token_bought_amount_raw / power(10, erc20_bought.decimals) AS token_bought_amount,
        base.token_sold_amount_raw / power(10, erc20_sold.decimals) AS token_sold_amount,
        base.token_bought_amount_raw,
        base.token_sold_amount_raw,
        coalesce(
                base.token_bought_amount_raw / power(10, erc20_bought.decimals) * p_bought.price,
                base.token_sold_amount_raw / power(10, erc20_sold.decimals) * p_sold.price
            ) AS amount_usd,
        base.token_bought_address,
        base.token_sold_address,
        coalesce(base.taker, base.tx_from) AS taker,
        base.maker,
        base.project_contract_address,
        base.tx_hash,
        base.tx_from,
        base.tx_to,
        base.evt_index
    FROM {{ base_trades }} base
    LEFT JOIN {{ tokens_erc20_model }} erc20_bought
        ON erc20_bought.contract_address = base.token_bought_address
        AND erc20_bought.blockchain = base.blockchain
    LEFT JOIN {{ tokens_erc20_model }} erc20_sold
        ON erc20_sold.contract_address = base.token_sold_address
        AND erc20_sold.blockchain = base.blockchain
    LEFT JOIN prices as p_bought
        ON p_bought.minute = date_trunc('minute', base.block_time)
        AND p_bought.contract_address = base.token_bought_address
        AND p_bought.blockchain = base.blockchain
    LEFT JOIN prices as p_sold
        ON p_sold.minute = date_trunc('minute', base.block_time)
        AND p_sold.contract_address = base.token_sold_address
        AND p_sold.blockchain = base.blockchain
)

select * from enrichments
{% endmacro %}