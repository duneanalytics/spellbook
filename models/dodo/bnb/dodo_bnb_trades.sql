{{ config
(
    alias ='trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "dodoex",
                                    \'["scoffie"]\') }}'
)
}}
    
-- The first dodo contract on bsc was deployed on '2021-01-23 07:47' from the query
--select min(evt_block_time) from dodoex_bnb.DODO_evt_BuyBaseToken;
{% set project_start_date = '2021-01-23' %}


WITH dodoex_view_markets (market_contract_address, base_token_symbol, quote_token_symbol, base_token_address, quote_token_address) AS

( VALUES 
         (lower('0x6064dbd0ff10bfed5a797807042e9f63f18cfe10'), 'USDC', 'BUSD', lower('0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d'), lower('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56')),
         (lower('0xBe60d4c4250438344bEC816Ec2deC99925dEb4c7'), 'BUSD', 'USDT', lower('0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56'), lower('0x55d398326f99059fF775485246999027B3197955'))       
)

, dexs AS 
(
    -- dodoex v1 sell
        SELECT
            s.evt_block_time AS block_time,
            'DODO' AS project,
            '1' AS version,
            s.seller AS taker,
            '' AS maker,
            s.payBase AS token_bought_amount_raw,
            s.receiveQuote AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            m.base_token_address AS token_bought_address,
            m.quote_token_address AS token_sold_address,
            s.contract_address AS project_contract_address,
            s.evt_tx_hash AS tx_hash,
            '' AS trace_address,
            s.evt_index
        FROM
            {{ source('dodoex_bnb', 'DODO_evt_SellBaseToken')}} s
        LEFT JOIN dodoex_view_markets m
            on s.contract_address = m.market_contract_address
        WHERE s.seller <> '0x8f8dd7db1bda5ed3da8c9daf3bfa471c12d58486'
        {% if is_incremental() %}
        AND s.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}
    
        UNION ALL

        -- dodoex v1 buy
        SELECT
            b.evt_block_time AS block_time,
            'DODO' AS project,
            '1' AS version,
            b.buyer AS taker,
            '' AS maker,
            b.receiveBase AS token_bought_amount_raw,
            b.payQuote AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            m.base_token_address AS token_bought_address,
            m.quote_token_address AS token_sold_address,
            b.contract_address AS project_contract_address,
            b.evt_tx_hash AS tx_hash,
            '' AS trace_address,
            b.evt_index
        FROM
            {{ source('dodoex_bnb','DODO_evt_BuyBaseToken')}} b
        LEFT JOIN dodoex_view_markets m
            on b.contract_address = m.market_contract_address
        WHERE b.buyer <> '0x8f8dd7db1bda5ed3da8c9daf3bfa471c12d58486'
        {% if is_incremental() %}
        AND b.evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}


        UNION ALL

        -- dodoex routeproxy
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            sender AS taker,
            '' AS maker,
            fromAmount AS token_bought_amount_raw,
            returnAmount AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodoex_bnb','DODORouteProxy_evt_OrderHistory')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        UNION ALL


        -- dodoex v2 proxy01
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            sender AS taker,
            '' AS maker,
            fromAmount AS token_bought_amount_raw,
            returnAmount AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodoex_bnb','DODOV2Proxy01_evt_OrderHistory')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        UNION ALL

        -- dodoex v2 proxy02
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            sender AS taker,
            '' AS maker,
            fromAmount AS token_bought_amount_raw,
            returnAmount AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodoex_bnb','DODOV2Proxy02_evt_OrderHistory')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}


        UNION ALL

        -- dodov2 dvm
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            trader AS taker,
            receiver AS maker,
            fromAmount AS token_bought_amount_raw,
            toAmount AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodoex_bnb', 'DVM_evt_DODOSwap')}}
        WHERE trader <> '0x8f8dd7db1bda5ed3da8c9daf3bfa471c12d58486'
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

        UNION ALL

        -- dodoex v2 dpp
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            trader AS taker,
            receiver AS maker,
            fromAmount AS token_bought_amount_raw,
            toAmount AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodoex_bnb', 'DPP_evt_DODOSwap')}}
        WHERE trader <> '0x8f8dd7db1bda5ed3da8c9daf3bfa471c12d58486'
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}


        UNION ALL 
        
        --dodoex dpporacle
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            trader AS taker,
            receiver AS maker,
            fromAmount AS token_bought_amount_raw,
            toAmount AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodoex_bnb', 'DPPOracle_evt_DODOSwap')}}
        WHERE trader <> '0x8f8dd7db1bda5ed3da8c9daf3bfa471c12d58486'
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}



        UNION ALL

        -- dodoex v2 dsp
        SELECT
            evt_block_time AS block_time,
            'DODO' AS project,
            '2' AS version,
            trader AS taker,
            receiver AS maker,
            fromAmount AS token_bought_amount_raw,
            toAmount AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            fromToken AS token_bought_address,
            toToken AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash,
            '' AS trace_address,
            evt_index
        FROM
            {{ source('dodoex_bnb', 'DSP_evt_DODOSwap')}}
        WHERE trader <> '0x8f8dd7db1bda5ed3da8c9daf3bfa471c12d58486'
        {% if is_incremental() %}
        AND evt_block_time >= date_trunc("day", now() - interval '1 week')
        {% endif %}

)

SELECT
    'bnb' AS blockchain
    ,project
    ,version
    ,TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,CAST(dexs.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw
    ,CAST(dexs.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw
    ,coalesce(
          dexs.amount_usd
        , (dexs.token_bought_amount_raw
            / power(10, (CASE dexs.token_bought_address
                             WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18
                             ELSE p_bought.decimals
                END))
              )
            * (CASE dexs.token_bought_address
                   WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN p_bnb.price
                   ELSE p_bought.price
                END)
        , (dexs.token_sold_amount_raw
            / power(10, (CASE dexs.token_sold_address
                             WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18
                             ELSE p_sold.decimals
                END))
              )
            * (CASE dexs.token_sold_address
                   WHEN '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN p_bnb.price
                   ELSE p_sold.price
                END)
    ) as amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx.from) AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx.from AS tx_from
    ,tx.to AS tx_to
    ,dexs.trace_address
    ,dexs.evt_index
FROM dexs
INNER JOIN {{ source('bnb', 'transactions')}} tx
    ON dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'bnb'
LEFT JOIN {{ ref('tokens_erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_bnb
    ON p_bnb.minute = date_trunc('minute', dexs.block_time)
    AND p_bnb.blockchain is null
    AND p_bnb.symbol = 'BNB'
    {% if not is_incremental() %}
    AND p_bnb.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bnb.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
;