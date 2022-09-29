{{ config(
    alias ='airswap_trades'
    )
}}

WITH dexs AS
(
    SELECT
        dexs.block_time,
        erc20a.symbol AS token_a_symbol,
        erc20b.symbol AS token_b_symbol,
        token_a_amount_raw / power(10, erc20a.decimals) AS token_a_amount,
        token_b_amount_raw / power(10, erc20b.decimals) AS token_b_amount,
        project,
        version,
        category,
        coalesce(trader_a, tx.`from`) AS trader_a, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        coalesce(
            usd_amount,
            token_a_amount_raw / power(10, pa.decimals) * pa.price,
            token_b_amount_raw / power(10, pb.decimals) * pb.price
        ) AS usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx.`from` AS tx_from,
        tx.`to` AS tx_to,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY tx_hash, evt_index, trace_address ORDER BY evt_index) AS trade_id
    FROM (
        SELECT
            evt_block_time AS block_time,
            'airswap' AS project,
            'light' AS version,
            'DEX' AS category,
            `senderWallet` AS trader_a, --define taker AS trader a
            `signerWallet` AS trader_b, --define maker AS trader b
            `senderAmount` AS token_a_amount_raw,
            `signerAmount` AS token_b_amount_raw,
            `senderToken` AS token_a_address,
            `signerToken` AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL AS trace_address,
            NULL AS usd_amount,
            evt_index
        FROM {{ source('airswap_ethereum', 'Light_evt_Swap')}} e

        UNION ALL

        SELECT
            evt_block_time AS block_time,
            'airswap' AS project,
            'swap' AS version,
            'DEX' AS category,
            `senderWallet` AS trader_a, --define taker AS trader a
            `signerWallet` AS trader_b, --define maker AS trader b
            `senderAmount` AS token_a_amount_raw,
            `signerAmount` AS token_b_amount_raw,
            `senderToken` AS token_a_address,
            `signerToken` AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL AS trace_address,
            NULL AS usd_amount,
            evt_index
        FROM {{ source('airswap_ethereum', 'swap_evt_Swap')}} e

        UNION ALL

        SELECT
            evt_block_time AS block_time,
            'airswap' AS project,
            'swap_v3' AS version,
            'DEX' AS category,
            `senderWallet` AS trader_a, --define taker AS trader a
            `signerWallet` AS trader_b, --define maker AS trader b
            `senderAmount` AS token_a_amount_raw,
            `signerAmount` AS token_b_amount_raw,
            `senderToken` AS token_a_address,
            `signerToken` AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            NULL AS trace_address,
            NULL AS usd_amount,
            evt_index
        FROM {{ source('airswap_ethereum', 'Swap_v3_evt_Swap')}} e
    ) dexs
    INNER JOIN {{ source('ethereum', 'transactions') }} tx
        ON dexs.tx_hash = tx.hash
        AND tx.block_time >= "2022-05-05 00:00:00"
        AND tx.block_time < current_timestamp()
        AND tx.block_number >= 0
        AND tx.block_number < 9e18
    LEFT JOIN {{ ref('tokens_erc20') }} erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN {{ ref('tokens_erc20') }} erc20b ON erc20b.contract_address = dexs.token_b_address
    LEFT JOIN {{ source('prices', 'usd') }} pa ON pa.minute = date_trunc('minute', dexs.block_time)
        AND pa.contract_address = dexs.token_a_address
        AND pa.minute >= "2022-05-05 00:00:00"
        AND pa.minute < current_timestamp()
    LEFT JOIN prices.usd pb ON pb.minute = date_trunc('minute', dexs.block_time)
        AND pb.contract_address = dexs.token_b_address
        AND pb.minute >= "2022-05-05 00:00:00"
        AND pb.minute < current_timestamp()
    WHERE dexs.block_time >= "2022-05-05 00:00:00"
    AND dexs.block_time < current_timestamp()

)

SELECT
    'ethereum' AS blockchain,
    project,
    version,
    TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    token_a_symbol AS token_bought_symbol,
    token_b_symbol AS token_sold_symbol,
    case
        when lower(token_a_symbol) > lower(token_b_symbol) then concat(token_b_symbol, '-', token_a_symbol)
        else concat(token_a_symbol, '-', token_b_symbol)
    end AS token_pair,
    token_a_amount AS token_bought_amount,
    token_b_amount AS token_sold_amount,
    token_a_amount_raw AS token_bought_amount_raw,
    token_b_amount_raw AS token_sold_amount_raw,
    usd_amount AS amount_usd,
    token_a_address AS token_bought_address,
    token_a_address AS token_sold_address,
    trader_a AS taker,
    trader_b AS maker,
    exchange_contract_address AS project_contract_address,
    tx_hash,
    tx_from,
    tx_to,
    trace_address,
    evt_index,
    'airswap' ||'-'|| dexs.tx_hash ||'-'|| IFNULL(dexs.evt_index, '') ||'-'|| IFNULL(dexs.trace_address, '') AS unique_trade_id
FROM dexs