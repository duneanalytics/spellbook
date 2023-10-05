{% macro dex_enrich_trades(
    blockchain='',
    models=[],
    transactions_model=null,
    tokens_erc20_model=null,
    prices_model=null
    )
%}

WITH base_union AS (
    {% for dex_model in models %}
    SELECT
        '{{ blockchain }}' as blockchain,
        '{{ dex_model[0] }}' as project,
        '{{ dex_model[1] }}' as version,
        block_date,
        block_month,
        block_time,
        token_bought_amount_raw,
        token_sold_amount_raw,
        amount_usd,
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        evt_index
    FROM {{ dex_model[2] }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
),

enrichments AS (
    SELECT
        base.blockchain,
        base.project,
        base.version,
        base.block_date,
        base.block_month,
        base.block_time,
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
                base.amount_usd,
                base.token_bought_amount_raw / power(10, erc20_bought.decimals) * p_bought.price,
                base.token_sold_amount_raw / power(10, erc20_sold.decimals) * p_sold.price
            ) AS amount_usd,
        base.token_bought_address,
        base.token_sold_address,
        coalesce(base.taker, tx."from") AS taker,
        base.maker,
        base.project_contract_address,
        base.tx_hash,
        tx."from" AS tx_from,
        tx.to AS tx_to,
        base.evt_index
    FROM base_union base
    INNER JOIN {{ transactions_model }} tx
        ON tx.hash = base.tx_hash
        {% if is_incremental() %}
        AND {{incremental_predicate('tx.block_time')}}
        {% endif %}
    LEFT JOIN {{ tokens_erc20_model }} erc20_bought
        ON erc20_bought.contract_address = base.token_bought_address
        AND erc20_bought.blockchain = '{{ blockchain }}'
    LEFT JOIN {{ tokens_erc20_model }} erc20_sold
        ON erc20_sold.contract_address = base.token_sold_address
        AND erc20_sold.blockchain = '{{ blockchain }}'
    LEFT JOIN {{ prices_model }} p_bought
        ON p_bought.minute = date_trunc('minute', base.block_time)
        AND p_bought.contract_address = base.token_bought_address
        AND p_bought.blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
        AND {{incremental_predicate('p_bought.minute')}}
        {% endif %}
    LEFT JOIN {{ prices_model }} p_sold
        ON p_sold.minute = date_trunc('minute', base.block_time)
        AND p_sold.contract_address = base.token_sold_address
        AND p_sold.blockchain = '{{ blockchain }}'
        {% if is_incremental() %}
        AND {{incremental_predicate('p_sold.minute')}}
        {% endif %}
)

select * from enrichments
{% endmacro %}
