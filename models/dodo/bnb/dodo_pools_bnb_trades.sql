{{ config
(
    
    alias = 'pool_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                    "project",
                                    "dodo",
                                    \'["owen05"]\') }}'
)
}}
    
{% set project_start_date = '2021-02-22' %}

WITH dodo_view_markets (market_contract_address, base_token_symbol, quote_token_symbol, base_token_address, quote_token_address) AS 
(
    VALUES
    (0x327134dE48fcDD75320f4c32498D1980470249ae, 'WBNB', 'BUSD', 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
    (0x5BDCf4962FDED6B7156E710400F4C4c031f600dC, 'KOGE', 'WBNB', 0xe6DF05CE8C8301223373CF5B969AFCb1498c5528, 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c),
    (0xBe60d4c4250438344bEC816Ec2deC99925dEb4c7, 'BUSD', 'USDT', 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56, 0x55d398326f99059fF775485246999027B3197955),
    (0xC64a1d5C819B3c9113cE3DB32B66D5D2b05B4CEf, 'BTCB', 'BUSD', 0x7130d2A12B9BCbFAe4f2634d864A1Ee1Ce3Ead9c, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
    (0x89E5015ff12E4536691aBfe5f115B1cB37a35465, 'ETH', 'BUSD', 0x2170Ed0880ac9A755fd29B2688956BD959F933F8, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
    (0x6064DBD0fF10BFeD5a797807042e9f63F18Cfe10, 'USDC', 'BUSD', 0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
    (0xb1327B6402ddbA34584Ab59fbe8Ac7cbF43f6353, 'DOT', 'BUSD', 0x7083609fCE4d1d8Dc0C979AAb8c869Ea2C873402,0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
    (0x8d078451a63D118bACC9Cc46698cc416f81C93E2, 'LINK', 'BUSD', 0xF8A0BF9cF54Bb92F17374d9e9A321E6a111a51bD, 0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56),
    (0x82AfF931d74F0645Ce80e8f419b94c8F93952686, 'WBNB', 'USDT', 0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c, 0x55d398326f99059fF775485246999027B3197955)
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
            {{ source('dodoex_bnb', 'DODO_evt_SellBaseToken')}} s
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
            {{ source('dodoex_bnb','DODO_evt_BuyBaseToken')}} b
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
            {{ source('dodoex_bnb', 'DVM_evt_DODOSwap')}}
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
            {{ source('dodoex_bnb', 'DPP_evt_DODOSwap')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL

        -- dodov2 dppAdvanced
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
            {{ source('dodoex_bnb', 'DPPAdvanced_evt_DODOSwap')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

        UNION ALL

        -- dodov2 dppOracle
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
            {{ source('dodoex_bnb', 'DPPOracle_evt_DODOSwap')}}
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
            {{ source('dodoex_bnb', 'DSP_evt_DODOSwap')}}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
)
SELECT
    'bnb' AS blockchain
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
INNER JOIN {{ source('bnb', 'transactions')}} tx
    ON dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'bnb'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_eth
    ON p_eth.minute = date_trunc('minute', dexs.block_time)
    AND p_eth.blockchain is null
    AND p_eth.symbol = 'BNB'
    {% if not is_incremental() %}
    AND p_eth.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_eth.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
WHERE dexs.token_bought_address <> dexs.token_sold_address