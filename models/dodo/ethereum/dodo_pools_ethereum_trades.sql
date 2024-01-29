{{ config
(
    
    alias = 'pool_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "dodo",
                                    \'["scoffie","owen05"]\') }}'
)
}}
    
-- The first dodo contracted was deployed on '2020-08-10 13:19' from the query
--select min(evt_block_time) from dodo_ethereum.DODO_evt_BuyBaseToken;
{% set project_start_date = '2020-08-10' %}

WITH dodo_view_markets (market_contract_address, base_token_symbol, quote_token_symbol, base_token_address, quote_token_address) AS 
(
    VALUES
    (0x75c23271661d9d143dcb617222bc4bec783eff34, 'WETH', 'USDC', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
    (0x562c0b218cc9ba06d9eb42f3aef54c54cc5a4650, 'LINK', 'USDC', 0x514910771af9ca656af840dff83e8264ecf986ca, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
    (0x0d04146b2fe5d267629a7eb341fb4388dcdbd22f, 'COMP', 'USDC', 0xc00e94cb662c3520282e6f5717214004a7f26888, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
    (0xca7b0632bd0e646b0f823927d3d2e61b00fe4d80, 'SNX', 'USDC',  0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
    (0xc226118fcd120634400ce228d61e1538fb21755f, 'LEND', 'USDC', 0x80fb784b7ed66730e8b1dbd9820afd29931aab03, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
    (0x2109f78b46a789125598f5ad2b7f243751c2934d, 'WBTC', 'USDC', 0x2260fac5e5542a773aa44fbcfedf7c193bc2c599, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
    (0x1b7902a66f133d899130bf44d7d879da89913b2e, 'YFI', 'USDC',  0x0bc529c00c6401aef6d220be8c6ea1667f6ad93e, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
    (0x1a7fe5d6f0bb2d071e16bdd52c863233bbfd38e9, 'WETH', 'USDT', 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, 0xdac17f958d2ee523a2206206994597c13d831ec7),
    (0xc9f93163c99695c6526b799ebca2207fdf7d61ad, 'USDT', 'USDC', 0xdac17f958d2ee523a2206206994597c13d831ec7, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
    (0xd4a36b0acfe2931cf922ea3d91063ddfe4aff01f, 'sUSD', 'USDT', 0x57ab1ec28d129707052df4df418d58a2d46d5f51, 0xdac17f958d2ee523a2206206994597c13d831ec7),
    (0x8876819535b48b551c9e97ebc07332c7482b4b2d, 'DODO', 'USDT', 0x43dfc4159d86f3a37a5a4b3d4580b888ad7d4ddd, 0xdac17f958d2ee523a2206206994597c13d831ec7),
    (0x9d9793e1e18cdee6cf63818315d55244f73ec006, 'FIN', 'USDT',  0x054f76beed60ab6dbeb23502178c52d6c5debe40, 0xdac17f958d2ee523a2206206994597c13d831ec7),
    (0x94512fd4fb4feb63a6c0f4bedecc4a00ee260528, 'AAVE', 'USDC', 0x7fc66500c84a76ad7e9c93437bfc5ac33e2ddae9, 0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48),
    (0x85f9569b69083c3e6aeffd301bb2c65606b5d575, 'wCRESt','USDT',0xa0afaa285ce85974c3c881256cb7f225e3a1178a, 0xdac17f958d2ee523a2206206994597c13d831ec7),
    (0x181D93EA28023bf40C8bB94796c55138719803B4, 'WOO','USDT', 0x4691937a7508860F876c9c0a2a617E7d9E945D4B, 0xdAC17F958D2ee523a2206206994597C13D831ec7),
    (0xd48c86156D53c0F775f40883391a113fC0D690d0, 'ibEUR','USDT', 0x96E61422b6A9bA0e068B6c5ADd4fFaBC6a4aae27, 0xdAC17F958D2ee523a2206206994597C13D831ec7)
)
, dexs AS 
(
        -- dodo v1 sell
        SELECT
            s.evt_block_time AS block_time,
            'dodo' AS project,
            '1' AS version,
            s.seller AS taker,
            CAST(NULL AS VARBINARY) AS maker,
            s.payBase AS token_bought_amount_raw,
            s.receiveQuote AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            m.base_token_address AS token_bought_address,
            m.quote_token_address AS token_sold_address,
            s.contract_address AS project_contract_address,
            s.evt_tx_hash AS tx_hash,
            s.evt_index
        FROM
            {{ source('dodo_ethereum', 'DODO_evt_SellBaseToken')}} s
        LEFT JOIN dodo_view_markets m
            on s.contract_address = m.market_contract_address
        {% if is_incremental() %}
        WHERE s.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    
        UNION ALL

        -- dodo v1 buy
        SELECT
            b.evt_block_time AS block_time,
            'dodo' AS project,
            '1' AS version,
            b.buyer AS taker,
            CAST(NULL AS VARBINARY) AS maker,
            b.receiveBase AS token_bought_amount_raw,
            b.payQuote AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            m.base_token_address AS token_bought_address,
            m.quote_token_address AS token_sold_address,
            b.contract_address AS project_contract_address,
            b.evt_tx_hash AS tx_hash,
            b.evt_index
        FROM
            {{ source('dodo_ethereum','DODO_evt_BuyBaseToken')}} b
        LEFT JOIN dodo_view_markets m
            on b.contract_address = m.market_contract_address
        {% if is_incremental() %}
        WHERE b.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL

        -- dodov2 dvm
        SELECT
            evt_block_time AS block_time,
            'dodo' AS project,
            '2_dvm' AS version,
            trader AS taker,
            receiver AS maker,
            fromAmount AS token_bought_amount_raw,
            toAmount AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            evt_index
        FROM
            {{ source('dodo_ethereum', 'DVM_evt_DODOSwap')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL

        -- dodov2 dpp
        SELECT
            evt_block_time AS block_time,
            'dodo' AS project,
            '2_dpp' AS version,
            trader AS taker,
            receiver AS maker,
            fromAmount AS token_bought_amount_raw,
            toAmount AS token_sold_amount_raw,
            cast(NULL as double)  AS amount_usd,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            evt_index
        FROM
            {{ source('dodo_ethereum', 'DPP_evt_DODOSwap')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL

        -- dodov2 dsp
        SELECT
            evt_block_time AS block_time,
            'dodo' AS project,
            '2_dsp' AS version,
            trader AS taker,
            receiver AS maker,
            fromAmount AS token_bought_amount_raw,
            toAmount AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            evt_index
        FROM
            {{ source('dodo_ethereum', 'DSP_evt_DODOSwap')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)
SELECT
    'ethereum' AS blockchain
    ,project
    ,dexs.version as version
    ,TRY_CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    ,TRY_CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,dexs.token_bought_amount_raw  AS token_bought_amount_raw
    ,dexs.token_sold_amount_raw  AS token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, (CASE dexs.token_bought_address WHEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 18 ELSE p_bought.decimals END))) * (CASE dexs.token_bought_address WHEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN  p_eth.price ELSE p_bought.price END)
        ,(dexs.token_sold_amount_raw / power(10, (CASE dexs.token_sold_address WHEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN 18 ELSE p_sold.decimals END))) * (CASE dexs.token_sold_address WHEN 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee THEN  p_eth.price ELSE p_sold.price END)
    ) as amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx."from") AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx."from" AS tx_from
    ,tx.to AS tx_to
    ,dexs.evt_index
FROM dexs
INNER JOIN {{ source('ethereum', 'transactions')}} tx
    ON dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'ethereum'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'ethereum'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'ethereum'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_eth
    ON p_eth.minute = date_trunc('minute', dexs.block_time)
    AND p_eth.blockchain is null
    AND p_eth.symbol = 'ETH'
    {% if not is_incremental() %}
    AND p_eth.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_eth.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
WHERE dexs.token_bought_address <> dexs.token_sold_address