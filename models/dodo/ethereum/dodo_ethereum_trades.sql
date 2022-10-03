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

WITH dodo_view_markets (market_contract_address, base_token_symbol, quote_token_symbol, base_token_address, quote_token_address) AS (VALUES
(lower('0x75c23271661d9d143dcb617222bc4bec783eff34'), 'WETH', 'USDC', lower('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'), lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')),
(lower('0x562c0b218cc9ba06d9eb42f3aef54c54cc5a4650'), 'LINK', 'USDC', lower('0x514910771af9ca656af840dff83e8264ecf986ca'), lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')),
(lower('0x0d04146b2fe5d267629a7eb341fb4388dcdbd22f'), 'COMP', 'USDC', lower('0xc00e94cb662c3520282e6f5717214004a7f26888'), lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')),
(lower('0xca7b0632bd0e646b0f823927d3d2e61b00fe4d80'), 'SNX', 'USDC',  lower('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f'), lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')),
(lower('0xc226118fcd120634400ce228d61e1538fb21755f'), 'LEND', 'USDC', lower('0x80fb784b7ed66730e8b1dbd9820afd29931aab03'), lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')),
(lower('0x2109f78b46a789125598f5ad2b7f243751c2934d'), 'WBTC', 'USDC', lower('0x2260fac5e5542a773aa44fbcfedf7c193bc2c599'), lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')),
(lower('0x1b7902a66f133d899130bf44d7d879da89913b2e'), 'YFI', 'USDC',  lower('0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e'), lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')),
(lower('0x1a7fe5d6f0bb2d071e16bdd52c863233bbfd38e9'), 'WETH', 'USDT', lower('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'), lower('0xdac17f958d2ee523a2206206994597c13d831ec7')),
(lower('0xc9f93163c99695c6526b799ebca2207fdf7d61ad'), 'USDT', 'USDC', lower('0xdac17f958d2ee523a2206206994597c13d831ec7'), lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')),
(lower('0xd4a36b0acfe2931cf922ea3d91063ddfe4aff01f'), 'sUSD', 'USDT', lower('0x57ab1ec28d129707052df4df418d58a2d46d5f51'), lower('0xdac17f958d2ee523a2206206994597c13d831ec7')),
(lower('0x8876819535b48b551c9e97ebc07332c7482b4b2d'), 'DODO', 'USDT', lower('0x43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd'), lower('0xdac17f958d2ee523a2206206994597c13d831ec7')),
(lower('0x9d9793e1e18cdee6cf63818315d55244f73ec006'), 'FIN', 'USDT',  lower('0x054f76beed60ab6dbeb23502178c52d6c5debe40'), lower('0xdac17f958d2ee523a2206206994597c13d831ec7')),
(lower('0x94512fd4fb4feb63a6c0f4bedecc4a00ee260528'), 'AAVE', 'USDC', lower('0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9'), lower('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')),
(lower('0x85f9569b69083c3e6aeffd301bb2c65606b5d575'), 'wCRESt','USDT',lower('0xa0afaa285ce85974c3c881256cb7f225e3a1178a'), lower('0xdac17f958d2ee523a2206206994597c13d831ec7')),
(lower('0x3058ef90929cb8180174d74c507176cca6835d73'), 'DAI', 'USDT',  lower('0x6b175474e89094c44da98b954eedeac495271d0f'), lower('0xdac17f958d2ee523a2206206994597c13d831ec7')),
(lower('0xd84820f0e66187c4f3245e1fe5ccc40655dbacc9'), 'sUSD', 'USDT', lower('0x57ab1ec28d129707052df4df418d58a2d46d5f51'), lower('0xdac17f958d2ee523a2206206994597c13d831ec7'))
)

,dexs AS 
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
        LEFT JOIN dodo_view_markets m on s.contract_address = m.market_contract_address
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
        LEFT JOIN dodo_view_markets m on b.contract_address = m.market_contract_address
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
        WHERE trader <> '0xa356867fdcea8e71aeaf87805808803806231fdc'
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
