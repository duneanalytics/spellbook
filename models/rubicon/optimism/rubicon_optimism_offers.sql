{{ config(
    schema = 'rubicon_optimism',
    
    alias = 'offers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "rubicon",
                                \'["denver"]\') }}'
    )
}}

-- First trade event at timestamp 1636432841 (2021-11-09 04:40:41)
{% set project_start_date = '2021-11-09' %}
   
WITH offers AS 
(
    -- select the offer data from the logMake event
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
    FROM {{ source('rubicon_optimism', 'RubiconMarket_evt_LogMake') }} e

    -- filter out offers that were created before the project start date
    WHERE e.evt_block_time >= cast('{{ project_start_date }}' AS timestamp)
    {% if is_incremental() %} -- only run this filter if it is an incremental run
    AND e.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION

    -- select the offer data from the emitOffer event
    SELECT 
        o.evt_block_time AS block_time,
        o.evt_block_number AS block_number,
        o.pay_gem AS sell_token_address,
        o.buy_gem AS buy_token_address,
        o.pay_amt AS sell_amount_raw,
        o.buy_amt AS buy_amount_raw,
        o.contract_address AS project_contract_address,
        o.evt_tx_hash AS tx_hash,
        o.evt_index,
        o.maker,
        o.id AS offer_id
    FROM {{ source('rubicon_optimism', 'RubiconMarket_evt_emitOffer') }} o

    -- filter out offers that were created before the project start date
    WHERE o.evt_block_time >= cast('{{ project_start_date }}' AS timestamp)
    {% if is_incremental() %} -- only run this filter if it is an incremental run
    AND o.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

raw_trades as (
    -- select the trade data from the logTake event
    SELECT
        t1.id AS offer_id,
        t1.pay_gem AS sell_token_address,
        t1.buy_gem AS buy_token_address,
        t1.evt_block_time,
        CAST(t1.take_amt as uint256) AS sold_amount_raw,
        CAST(t1.give_amt as uint256) AS bought_amount_raw
    FROM {{ source('rubicon_optimism', 'RubiconMarket_evt_LogTake') }} t1 

    UNION

    -- select the trade data from the emitTake event
    SELECT
        t2.id AS offer_id,
        t2.pay_gem AS sell_token_address,
        t2.buy_gem AS buy_token_address,
        t2.evt_block_time,
        CAST(t2.take_amt as uint256) AS sold_amount_raw,
        CAST(t2.give_amt as uint256) AS bought_amount_raw
    FROM {{ source('rubicon_optimism', 'RubiconMarket_evt_emitTake') }} t2
),

trades AS 
(

    SELECT t.*,  
        erc20_sell.symbol AS sell_token_symbol,
        erc20_buy.symbol AS buy_token_symbol,
        CAST(t.sold_amount_raw as uint256) / power(10, erc20_sell.decimals) AS sold_amount,
        CAST(t.bought_amount_raw as uint256) / power(10, erc20_buy.decimals) AS bought_amount,
        (CAST(t.sold_amount_raw as uint256) / power(10, erc20_sell.decimals)) * sell_token_price.price AS sold_amount_usd,
        (CAST(t.bought_amount_raw as uint256) / power(10, erc20_buy.decimals)) * buy_token_price.price AS bought_amount_usd
    FROM raw_trades t

    -- get the relevant sell token data
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_sell
        ON erc20_sell.contract_address = t.sell_token_address
        AND erc20_sell.blockchain = 'optimism'

    -- get the relevant buy token data
    LEFT JOIN {{ source('tokens', 'erc20') }} erc20_buy
        ON erc20_buy.contract_address = t.buy_token_address
        AND erc20_buy.blockchain = 'optimism'

    -- get the sell token price
    LEFT JOIN {{ source('prices', 'usd') }} sell_token_price
        ON sell_token_price.minute = date_trunc('minute', t.evt_block_time)
        AND sell_token_price.contract_address = t.sell_token_address
        AND sell_token_price.blockchain = 'optimism'
        {% if not is_incremental() %} -- only run this filter if it is an incremental run
        AND sell_token_price.minute >= cast('{{ project_start_date }}' AS timestamp)
        {% endif %}
    
    -- get the buy token price
    LEFT JOIN {{ source('prices', 'usd') }} buy_token_price
        ON buy_token_price.minute = date_trunc('minute', t.evt_block_time)
        AND buy_token_price.contract_address = t.buy_token_address
        AND buy_token_price.blockchain = 'optimism'
        {% if not is_incremental() %} -- only run this filter if it is an incremental run
        AND buy_token_price.minute >= cast('{{ project_start_date }}' AS timestamp)
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
    'optimism' AS blockchain,
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
    txn.gas_price AS gas_price,
    txn.gas_used AS gas_used,
    txn.l1_gas_price AS l1_gas_price,
    txn.l1_gas_used AS l1_gas_used,
    txn.l1_fee_scalar AS l1_fee_scalar,
    ((CAST(txn.gas_used as uint256) / power(10, 18)) * CAST(txn.gas_price as uint256)) + ((CAST(txn.l1_gas_used as uint256) / power(10, 18)) * CAST(txn.l1_gas_price as uint256) * CAST(txn.l1_fee_scalar as uint256)) AS txn_cost_eth,
    eth.price AS eth_price,
    ((CAST(txn.gas_used as uint256) / power(10, 18)) * CAST(txn.gas_price as uint256)) + ((CAST(txn.l1_gas_used as uint256) / power(10, 18)) * CAST(txn.l1_gas_price as uint256) * CAST(txn.l1_fee_scalar as uint256)) * eth.price AS txn_cost_usd,
    offers.project_contract_address, 
    offers.tx_hash,
    txn."from" AS tx_from,
    txn.to AS tx_to
FROM offers

-- get the transaction data for the offer
INNER JOIN {{ source('optimism', 'transactions') }} txn
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
    AND erc20_sell.blockchain = 'optimism'

-- get the relevant buy token data
LEFT JOIN {{ source('tokens', 'erc20') }} erc20_buy
    ON erc20_buy.contract_address = offers.buy_token_address
    AND erc20_buy.blockchain = 'optimism'

-- get the price data for the sell token
LEFT JOIN {{ source('prices', 'usd') }} sell_token_price
    ON sell_token_price.minute = date_trunc('minute', offers.block_time)
    AND sell_token_price.contract_address = offers.sell_token_address
    AND sell_token_price.blockchain = 'optimism'
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
    AND buy_token_price.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND buy_token_price.minute >= cast('{{ project_start_date }}' AS timestamp)
    {% endif %}
    {% if is_incremental() %}
    AND buy_token_price.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}

-- get the price of eth at the time of the offer 
LEFT JOIN {{ source('prices', 'usd') }}  eth
    ON eth.minute = date_trunc('minute', offers.block_time)
    AND eth.contract_address = 0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000 -- this is for optimism specifically
    AND eth.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND eth.minute >= cast('{{ project_start_date }}' AS timestamp)
    {% endif %}
    {% if is_incremental() %}
    AND eth.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    
-- get the trades that filled the offer
LEFT JOIN trades_grouped trades
    ON trades.offer_id = offers.offer_id