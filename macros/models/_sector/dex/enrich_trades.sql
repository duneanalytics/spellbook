{% macro enrich_trades(
    blockchain='',
    models=[],
    transactions_model=null,
    tokens_erc20_model=null,
    prices_model=null,
    )
%}

WITH base_union AS (
    {% for dex_model in models %}
    SELECT
        '{{ blockchain }}' as blockchain,
        '{{ dex_model[0] }}' as project,
        '{{ dex_model[1] }}' as version,
        block_date,
        block_time,
        token_bought_amount_raw,
        token_sold_amount_raw,
        token_bought_address,
        token_sold_address,
        taker,
        maker,
        project_contract_address,
        tx_hash,
        trace_address,
        evt_index
    FROM {{ dex_model[2] }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
),

enrichments AS (
SELECT base.blockchain,
       base.project,
       base.version,
       base.block_date,
       base.block_time,
       erc20a.symbol                                             AS token_bought_symbol,
       erc20b.symbol                                             AS token_sold_symbol,
       case
           when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
           else concat(erc20a.symbol, '-', erc20b.symbol)
           end                                                   AS token_pair,
       base.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
       base.token_sold_amount_raw / power(10, erc20b.decimals)   AS token_sold_amount,
       base.token_bought_amount_raw,
       base.token_sold_amount_raw,
       coalesce(
               base.amount_usd,
               base.token_bought_amount_raw / power(10, erc20a.decimals) * pa.price,
               base.token_sold_amount_raw / power(10, erc20b.decimals) * pb.price
           )                                                     AS amount_usd,
       base.token_bought_address,
       base.token_sold_address,
       coalesce(base.taker, tx.from)                             AS taker,
       base.maker,
       base.project_contract_address,
       base.tx_hash,
       tx.from                                                   AS tx_from,
       tx.to                                                     AS tx_to,
       base.trace_address,
       base.evt_index
FROM base_union base
INNER JOIN {{ transactions_model }} tx
ON tx.block_number = base.block_number
    AND tx.hash = base.tx_hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ tokens_erc20_model }} erc20a
    ON erc20a.contract_address = base.token_bought_address
    AND erc20a.blockchain = '{{ blockchain }}'
LEFT JOIN {{ tokens_erc20_model }} erc20b
    ON erc20b.contract_address = base.token_sold_address
    AND erc20b.blockchain = '{{ blockchain }}'
LEFT JOIN {{ prices_model }} pa
    ON pa.minute = date_trunc('minute', base.block_time)
    AND pa.contract_address = base.token_bought_address
    AND pa.blockchain = '{{ blockchain }}'
    {% if is_incremental() %}
    AND pa.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ prices_model }} pb
    ON pb.minute = date_trunc('minute', base.block_time)
    AND pb.contract_address = base.token_sold_address
    AND pb.blockchain = '{{ blockchain }}'
    {% if is_incremental() %}
    AND pb.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)

select * from enrichments
{% endmacro %}
