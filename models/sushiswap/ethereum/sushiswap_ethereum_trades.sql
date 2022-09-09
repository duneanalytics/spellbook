{{ config(
    schema = 'sushiswap_ethereum_trades',
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id']
    )
}}

WITH (
        -- Sushiswap
        SELECT
            t.evt_block_time AS block_time,
            t."to" AS taker,
            '' AS maker,
            CASE WHEN "amount0Out" = 0 THEN "amount1Out" ELSE "amount0Out" END AS token_bought_amount_raw,
            CASE WHEN "amount0In" = 0 THEN "amount1In" ELSE "amount0In" END AS token_sold_amount_raw,
            NULL AS amount_usd,
            CASE WHEN "amount0Out" = 0 THEN f.token1 ELSE f.token0 END AS token_bought_address,
            CASE WHEN "amount0In" = 0 THEN f.token1 ELSE f.token0 END AS token_sold_address,
            t.contract_address project_contract_address,
            t.evt_tx_hash AS tx_hash,
            '' AS trace_address,
            t.evt_index
        FROM
            {{ source('sushiswap_ethereum', 'Pair_evt_Swap') }} t
        INNER JOIN {{ source('sushiswap_ethereum', 'Factory_evt_PairCreated') }} f ON f.pair = t.contract_address
    ) as dexs

    SELECT
        'ethereum' as blockchain,
        'sushiswap' as project,
        '' as version,
        TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
        dexs.block_time,
        erc20a.symbol AS token_bought_symbol,
        erc20b.symbol AS token_sold_symbol,
        token_bought_amount_raw / 10 ^ erc20a.decimals AS token_bought_amount,
        token_sold_amount_raw / 10 ^ erc20b.decimals AS token_sold_amount,
        case
            when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
            else concat(erc20a.symbol, '-', erc20b.symbol)
        end as token_pair,
        dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
        dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
        dexs.token_bought_amount_raw,
        dexs.token_sold_amount_raw,
        coalesce(
            dexs.amount_usd
            ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
            ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
        ) AS amount_usd,
        dexs.token_bought_address,
        dexs.token_sold_address,
        coalesce(dexs.taker, tx.from) AS taker,
        dexs.maker,
        dexs.project_contract_address,
        dexs.tx_hash,
        tx.from AS tx_from,
        tx.to AS tx_to,
        dexs.trace_address,
        dexs.evt_index,
        'sushiswap' ||'-'|| dexs.tx_hash ||'-'|| IFNULL(dexs.evt_index, '') ||'-'|| IFNULL(dexs.trace_address, '') AS unique_trade_id
    FROM dexs
    INNER JOIN {{ source('ethereum', 'transactions') }} tx
        ON dexs.tx_hash = tx.hash
        {% if not is_incremental() %}
        -- TODO: Determine minimum date for SS (using directional date of mainnet launch for SS)
        AND tx.block_time >= "2020-09-04 10:00:00"
        {% endif %}
        {% if is_incremental() %}
        AND tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ ref('tokens_erc20') }} erc20a ON erc20a.contract_address = dexs.token_bought_address AND erc20a.blockchain = 'ethereum'
    LEFT JOIN {{ ref('tokens_erc20') }} erc20b ON erc20b.contract_address = dexs.token_sold_address AND erc20b.blockchain = 'ethereum'
    LEFT JOIN {{ source('prices', 'usd') }} p_bought ON p_bought.minute = date_trunc('minute', dexs.block_time)
        AND p_bought.contract_address = dexs.token_bought_address
        {% if not is_incremental() %}
        -- TODO: Determine minimum date for SS (using directional date of mainnet launch for SS)
        AND p_bought.minute >= "2020-09-04 10:00:00"
        {% endif %}
        {% if is_incremental() %}
        AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    LEFT JOIN {{ source('prices', 'usd') }} p_sold ON pb.minute = date_trunc('minute', dexs.block_time)
        AND p_sold.contract_address = dexs.token_sold_address
        {% if not is_incremental() %}
        -- TODO: Determine minimum date for SS (using directional date of mainnet launch for SS)
        AND p_bought.minute >= "2020-09-04 10:00:00"
        {% endif %}
        {% if is_incremental() %}
        AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    WHERE dexs.block_time >= start_ts
    AND dexs.block_time < end_ts