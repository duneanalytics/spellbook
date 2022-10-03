{{ config(
    schema = 'dodo_ethereum',
    alias ='trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'category', 'tx_hash', 'evt_index', 'trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "dodo",
                                    \'["scoffie"]\') }}'

)
}}
                                    
-- The first dodo contracted was deployed on '2020-08-10 13:19' from the query
--`select min(evt_block_time) from dodo_ethereum.DODO_evt_BuyBaseToken;`
{% set project_start_date = '2020-08-10' %}
{% set query_start_block_number = 0 %}
{% set query_end_block_number = 9e18 %}

WITH dexs AS 
    (SELECT
        dexs.block_time,
        erc20a.symbol AS token_a_symbol,
        erc20b.symbol AS token_b_symbol,
        token_a_amount_raw / POW(10 ,erc20a.decimals) AS token_a_amount,
        token_b_amount_raw / POW(10 ,erc20b.decimals) AS token_b_amount,
        project,
        version,
        category,
        coalesce(trader_a, tx.`from`) as trader_a, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
        trader_b,
        token_a_amount_raw,
        token_b_amount_raw,
        coalesce(
            usd_amount,
            token_a_amount_raw / POW(10 , (CASE token_a_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE pa.decimals END)) * (CASE token_a_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN  pa.price ELSE pa.price END),
            token_b_amount_raw / POW(10 ,(CASE token_b_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE pb.decimals END)) * (CASE token_b_address WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN  pb.price ELSE pb.price END)
        ) as usd_amount,
        token_a_address,
        token_b_address,
        exchange_contract_address,
        tx_hash,
        tx.`from` as tx_from,
        tx.`to` as tx_to,
        trace_address,
        evt_index,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index, trace_address ORDER BY version, category) AS trade_id
    FROM (

        -- dodo v1 sell
        SELECT
            s.evt_block_time AS block_time,
            'DODO' AS project,
            '1' AS version,
            'DEX' AS category,
            s.`seller` AS trader_a,
            NULL AS trader_b,
            s.`payBase` token_a_amount_raw,
            s.`receiveQuote` token_b_amount_raw,
            cast(NULL as double)  AS usd_amount,
            m.base_token_address AS token_a_address,
            m.quote_token_address AS token_b_address,
            s.contract_address AS exchange_contract_address,
            s.evt_tx_hash AS tx_hash,
            '' AS trace_address,
            s.evt_index
        FROM
            {{ source('dodo_ethereum', 'DODO_evt_SellBaseToken')}} s
        LEFT JOIN {{ ref('dodo_view_markets') }} m on s.contract_address = m.market_contract_address
         {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
         {% endif %}
        WHERE s.seller <> '0xa356867fdcea8e71aeaf87805808803806231fdc'
       
        UNION ALL

        -- dodo v1 buy
        SELECT
            b.`evt_block_time` AS block_time,
            'DODO' AS project,
            '1' AS version,
            'DEX' AS category,
            b.`buyer` AS trader_a,
            NULL AS trader_b,
            b.`receiveBase` token_a_amount_raw,
            b.`payQuote` token_b_amount_raw,
            cast(NULL as double) AS usd_amount,
            m.base_token_address AS token_a_address,
            m.quote_token_address AS token_b_address,
            b.contract_address AS exchange_contract_address,
            b.evt_tx_hash AS tx_hash,
            '' AS trace_address,
            b.evt_index
        FROM
            {{ source('dodo_ethereum','DODO_evt_BuyBaseToken')}} b
        LEFT JOIN {{ ref ('dodo_view_markets') }}  m on b.contract_address = m.market_contract_address
         {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
         {% endif %}
        WHERE b.buyer <> '0xa356867fdcea8e71aeaf87805808803806231fdc'

        UNION ALL

        -- dodov1 proxy01
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '1' AS version,
            'Aggregator' AS category,
            `sender` AS trader_a,
            NULL AS trader_b,
            `fromAmount` token_a_amount_raw,
            `returnAmount` token_b_amount_raw,
            cast(NULL as double) AS usd_amount,
            `fromToken` AS token_a_address,
            `toToken` AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodo_ethereum' ,'DODOV1Proxy01_evt_OrderHistory')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
         {% endif %}

        UNION ALL

        -- dodov1 proxy04
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '1' AS version,
            'Aggregator' AS category,
            sender AS trader_a,
            NULL AS trader_b,
            `fromAmount` token_a_amount_raw,
            `returnAmount` token_b_amount_raw,
            cast(NULL as double) AS usd_amount,
            `fromToken` AS token_a_address,
            `toToken` AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodo_ethereum', 'DODOV1Proxy04_evt_OrderHistory')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        UNION ALL

        -- dodov2 proxy02
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            'Aggregator' AS category,
            sender AS trader_a,
            NULL AS trader_b,
            `fromAmount` token_a_amount_raw,
            `returnAmount` token_b_amount_raw,
            cast(NULL as double) AS usd_amount,
            `fromToken` AS token_a_address,
            `toToken` AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodo_ethereum','DODOV2Proxy02_evt_OrderHistory')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        UNION ALL

        -- dodov2 dvm
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            'DEX' AS category,
            trader AS trader_a,
            receiver AS trader_b,
            `fromAmount` AS token_a_amount_raw,
            `toAmount` AS token_b_amount_raw,
            cast(NULL as double) AS usd_amount,
            `fromToken` AS token_a_address,
            `toToken` AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodo_ethereum', 'DVM_evt_DODOSwap')}}

        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        WHERE trader <> '0xa356867fdcea8e71aeaf87805808803806231fdc'

        UNION ALL

        -- dodov2 dpp
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            'DEX' AS category,
            trader AS trader_a,
            receiver AS trader_b,
            `fromAmount` AS token_a_amount_raw,
            `toAmount` AS token_b_amount_raw,
            cast(NULL as double)  AS usd_amount,
            `fromToken` AS token_a_address,
            `toToken` AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodo_ethereum', 'DPP_evt_DODOSwap')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        WHERE trader <> '0xa356867fdcea8e71aeaf87805808803806231fdc'

        UNION ALL

        -- dodov2 dsp
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            'DEX' AS category,
            trader AS trader_a,
            receiver AS trader_b,
            `fromAmount` AS token_a_amount_raw,
            `toAmount` AS token_b_amount_raw,
            cast(NULL as double) AS usd_amount,
            `fromToken` AS token_a_address,
            `toToken` AS token_b_address,
            contract_address AS exchange_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodo_ethereum', 'DSP_evt_DODOSwap')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        WHERE trader <> '0\xa356867fdcea8e71aeaf87805808803806231fdc'
    ) dexs
    INNER JOIN {{ source('ethereum', 'transactions')}} tx
        ON dexs.tx_hash = tx.hash
         {% if not is_incremental() %}
        AND tx.block_time >= '{{project_start_date}}'
        {% endif %}
        AND tx.block_time < CURRENT_TIMESTAMP()
        {% if is_incremental() %}
        AND tx.block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND tx.block_number >={{query_start_block_number}}
        AND tx.block_number < {{query_end_block_number}}
    LEFT JOIN {{ ref('tokens_erc20') }} erc20a ON erc20a.contract_address = dexs.token_a_address
    LEFT JOIN {{ ref('tokens_erc20') }} erc20b ON erc20b.contract_address = dexs.token_b_address
    LEFT JOIN {{ source('prices', 'usd')}} pa ON pa.minute = date_trunc('minute', dexs.block_time)
        AND pa.contract_address = dexs.token_a_address
        {% if not is_incremental() %}
        AND pa.minute >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND pa.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND pa.minute < CURRENT_TIMESTAMP()
    LEFT JOIN {{ source('prices', 'usd')}} pb ON pb.minute = date_trunc('minute', dexs.block_time)
        AND pb.contract_address = dexs.token_b_address
        {% if not is_incremental() %}
        AND pb.minute >= '{{project_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND pb.minute >= date_trunc("day", now() - interval '1 week')
        {% endif %}
        AND pb.minute < CURRENT_TIMESTAMP()
        --FIGURING OUT A WAY TO INCLUDE THE prices.layer1_usd 
    -- LEFT JOIN prices.layer1_usd pe ON pe.minute = date_trunc('minute', dexs.block_time)
    --     AND pe.symbol = 'ETH'
    --     AND pe.minute >= '2020-08-10 13:19'
    --     AND pe.minute < CURRENT_TIMESTAMP()
    WHERE dexs.block_time >= '{{project_start_date}}'
    AND dexs.block_time < CURRENT_TIMESTAMP()
    AND dexs.token_a_address <> dexs.token_b_address
  )

SELECT
    'ethereum' AS blockchain,
    project,
    version,
    category,
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
    trade_id
FROM dexs
