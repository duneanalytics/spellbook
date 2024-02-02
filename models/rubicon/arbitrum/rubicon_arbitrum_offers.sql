{{ config(
    schema = 'rubicon_arbitrum',
    alias = 'offers',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "rubicon",
                                \'["denver"]\') }}'
    )
}}

{% set project_start_date = '2023-06-09' %}
   
WITH offers AS 
(
    -- select the offer data from the emitOffer event
    SELECT 
        e.evt_block_time AS block_time,
        e.evt_block_number AS block_number,
        e.pay_gem AS sell_token_address,
        e.buy_gem AS buy_token_address,
        e.pay_amt AS sell_amount_raw,
        e.buy_amt AS buy_amount_raw,
        e.contract_address AS project_contract_address,
        e.evt_tx_hash AS tx_hash,
        e.evt_index,
        e.maker,
        e.id AS offer_id
    FROM {{ source('rubicon_arbitrum', 'RubiconMarket_evt_emitOffer') }} e

    -- filter out offers that were created before the project start date
    WHERE e.evt_block_time >= cast('{{ project_start_date }}' AS timestamp)
    {% if is_incremental() %} -- only run this filter if it is an incremental run
    AND e.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

trades AS 
(
    -- select the trade data from the emitTake event
    SELECT
        t.id AS offer_id,
        t.pay_gem AS sell_token_address,
        t.buy_gem AS buy_token_address,
        CAST(t.take_amt as uint256) AS sold_amount_raw,
        CAST(t.give_amt as uint256) AS bought_amount_raw,
        erc20_sell.symbol AS sell_token_symbol,
        erc20_buy.symbol AS buy_token_symbol,
        CAST(t.take_amt as uint256) / power(10, erc20_sell.decimals) AS sold_amount,
        CAST(t.give_amt as uint256) / power(10, erc20_buy.decimals) AS bought_amount,
        (CAST(t.take_amt as uint256) / power(10, erc20_sell.decimals)) * sell_token_price.price AS sold_amount_usd,
        (CAST(t.give_amt as uint256) / power(10, erc20_buy.decimals)) * buy_token_price.price AS bought_amount_usd
    FROM {{ source('rubicon_arbitrum', 'RubiconMarket_evt_emitTake') }} t 

    -- get the relevant sell token data
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sell
        ON erc20_sell.contract_address = t.pay_gem
        AND erc20_sell.blockchain = 'arbitrum'

    -- get the relevant buy token data
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_buy
        ON erc20_buy.contract_address = t.buy_gem
        AND erc20_buy.blockchain = 'arbitrum'

    -- get the sell token price
    LEFT JOIN {{ source('prices', 'usd') }} sell_token_price
        ON sell_token_price.minute = date_trunc('minute', t.evt_block_time)
        AND sell_token_price.contract_address = t.pay_gem
        AND sell_token_price.blockchain = 'arbitrum'
        {% if not is_incremental() %} -- only run this filter if it is an incremental run
        AND sell_token_price.minute >= cast('{{ project_start_date }}' AS timestamp)
        {% endif %}
        {% if is_incremental() %}
        AND sell_token_price.minute >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    
    -- get the buy token price
    LEFT JOIN {{ source('prices', 'usd') }} buy_token_price
        ON buy_token_price.minute = date_trunc('minute', t.evt_block_time)
        AND buy_token_price.contract_address = t.buy_gem
        AND buy_token_price.blockchain = 'arbitrum'
        {% if not is_incremental() %} -- only run this filter if it is an incremental run
        AND buy_token_price.minute >= cast('{{ project_start_date }}' AS timestamp)
        {% endif %}
        {% if is_incremental() %}
        AND buy_token_price.minute >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    
    -- filter out trades that were created before the project start date
    WHERE t.evt_block_time >= cast('{{ project_start_date }}' AS timestamp) 
    {% if is_incremental() %} -- only run this filter if it is an incremental run
    AND t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

trades_grouped AS 
(
    -- group the trades by offer id
    SELECT 
        offer_id,
        sell_token_address,
        buy_token_address,
        sum(sold_amount_raw) AS sold_amount_raw,
        sum(bought_amount_raw) AS bought_amount_raw,
        sum(sold_amount) AS sold_amount,
        sum(bought_amount) AS bought_amount,
        sum(sold_amount_usd) AS sold_amount_usd,
        sum(bought_amount_usd) AS bought_amount_usd,
        sell_token_symbol,
        buy_token_symbol
    FROM trades
    GROUP BY offer_id, sell_token_address, buy_token_address, sell_token_symbol, buy_token_symbol
)

SELECT 

    -- aggregate date to match our schema
    'arbitrum' AS blockchain,
    'rubicon' AS project,
    '1' AS version,
    CAST(date_trunc('DAY', offers.block_time) AS date) AS block_date,
    CAST(date_trunc('MONTH', offers.block_time) AS date) AS block_month,
    offers.block_time,
    offers.block_number,
    txn.index AS tx_index,
    offers.evt_index,
    offers.maker,
    erc20_sell.symbol AS sell_token_symbol,
    erc20_buy.symbol AS buy_token_symbol, 
    offers.sell_token_address,
    offers.buy_token_address,
    CASE -- create a common pair format for all tokens
        WHEN lower(erc20_sell.symbol) > lower(erc20_buy.symbol) THEN concat(erc20_buy.symbol, '-', erc20_sell.symbol)
        ELSE concat(erc20_sell.symbol, '-', erc20_buy.symbol)
    END AS token_pair,
    CAST(offers.sell_amount_raw as uint256) / power(10, erc20_sell.decimals) AS sell_amount,
    cast(offers.buy_amount_raw as uint256) / power(10, erc20_buy.decimals) AS buy_amount,
    CAST(offers.sell_amount_raw as uint256) AS sell_amount_raw,
    CAST(offers.buy_amount_raw as uint256) AS buy_amount_raw,
    trades.sold_amount AS sold_amount,
    trades.bought_amount AS bought_amount,
    CAST(trades.sold_amount_raw as uint256) AS sold_amount_raw,
    CAST(trades.bought_amount_raw as uint256) AS bought_amount_raw,
    cast(offers.sell_amount_raw as uint256) / power(10, erc20_sell.decimals) * sell_token_price.price AS sell_amount_usd,
    cast(offers.buy_amount_raw as uint256) / power(10, erc20_buy.decimals) * buy_token_price.price AS buy_amount_usd,
    trades.sold_amount_usd AS sold_amount_usd,
    trades.bought_amount_usd AS bought_amount_usd,
    txn.effective_gas_price AS effective_gas_price,
    txn.gas_used AS gas_used,
    ((CAST(txn.effective_gas_price as uint256) / power(10, 18)) * CAST(txn.gas_used as uint256)) AS txn_cost_eth,
    eth.price AS eth_price,
    ((CAST(txn.effective_gas_price as uint256) / power(10, 18)) * CAST(txn.gas_used as uint256)) * eth.price AS txn_cost_usd,
    offers.project_contract_address, 
    offers.tx_hash,
    txn."from" AS tx_from,
    txn.to AS tx_to
FROM offers

-- get the transaction data for the offer
INNER JOIN {{ source('arbitrum', 'transactions') }} txn
    ON txn.hash = offers.tx_hash
    AND txn.block_number = offers.block_number
    {% if not is_incremental() %}
    AND txn.block_time >= cast('{{ project_start_date }}' AS timestamp)
    {% endif %}
    {% if is_incremental() %} -- only run this filter if it is an incremental run
    AND txn.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    
-- get the relevant sell token data
LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sell
    ON erc20_sell.contract_address = offers.sell_token_address
    AND erc20_sell.blockchain = 'arbitrum'

-- get the relevant buy token data
LEFT JOIN {{ source('tokens', 'erc20') }} erc20_buy
    ON erc20_buy.contract_address = offers.buy_token_address
    AND erc20_buy.blockchain = 'arbitrum'

-- get the price data for the sell token
LEFT JOIN {{ source('prices', 'usd') }} sell_token_price
    ON sell_token_price.minute = date_trunc('minute', offers.block_time)
    AND sell_token_price.contract_address = offers.sell_token_address
    AND sell_token_price.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND sell_token_price.minute >= cast('{{ project_start_date }}' AS timestamp)
    {% endif %}
    {% if is_incremental() %}
    AND sell_token_price.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    
-- get the price data for the buy token
LEFT JOIN {{ source('prices', 'usd') }} buy_token_price
    ON buy_token_price.minute = date_trunc('minute', offers.block_time)
    AND buy_token_price.contract_address = offers.buy_token_address
    AND buy_token_price.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND buy_token_price.minute >= cast('{{ project_start_date }}' AS timestamp)
    {% endif %}
    {% if is_incremental() %}
    AND buy_token_price.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}

-- get the price of eth at the time of the offer 
LEFT JOIN {{ source('prices', 'usd') }}  eth
    ON eth.minute = date_trunc('minute', offers.block_time)
    AND eth.contract_address = 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 -- this is for arbitrum specifically
    AND eth.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND eth.minute >= cast('{{ project_start_date }}' AS timestamp)
    {% endif %}
    {% if is_incremental() %}
    AND eth.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    
-- get the trades that filled the offer
LEFT JOIN trades_grouped trades
    ON trades.offer_id = offers.offer_id