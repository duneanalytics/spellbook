{{ config(
    schema = 'rubicon_optimism',
    alias = 'offers',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "rubicon",
                                \'["denver"]\') }}'
    )
}}

-- First swap event at timestamp 1636432841 (2021-11-09 04:40:41)
{% set project_start_date = '2021-11-09' %}

with offers as 
(
    -- select the offer data from the logMake event
    select 
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
        e.id as offer_id
    from {{ source('rubicon_optimism', 'RubiconMarket_evt_LogMake') }} e

    where e.evt_block_time >= '{{ project_start_date }}'
    {% if is_incremental() %}
    AND e.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
),

trades as 
(
    -- select the trade data from the logTake event
    select 
        t.id as offer_id,
        t.pay_gem as sell_token_address,
        t.buy_gem as buy_token_address,
        CAST(t.take_amt AS DECIMAL(38,0)) as sold_amount_raw,
        CAST(t.give_amt AS DECIMAL(38,0)) as bought_amount_raw,
        erc20_sell.symbol AS sell_token_symbol,
        erc20_buy.symbol AS buy_token_symbol,
        CAST(t.take_amt AS DECIMAL(38,0)) / power(10, erc20_sell.decimals) as sold_amount,
        CAST(t.give_amt AS DECIMAL(38,0)) / power(10, erc20_buy.decimals) as bought_amount,
        CAST(t.take_amt AS DECIMAL(38,0)) / power(10, erc20_sell.decimals) * sell_token_price.price as sold_amount_usd,
        CAST(t.give_amt AS DECIMAL(38,0)) / power(10, erc20_buy.decimals) * buy_token_price.price as bought_amount_usd,
    from {{ source('rubicon_optimism', 'RubiconMarket_evt_LogTake') }} t 
    where t.evt_block_time >= '{{ project_start_date }}'
    {% if is_incremental() %}
    AND t.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    left join {{ ref('tokens_erc20') }} erc20_sell
        on erc20_sell.contract_address = t.pay_gem
    left join {{ ref('tokens_erc20') }} erc20_buy
        on erc20_buy.contract_address = t.buy_gem
    left join {{ source('prices', 'usd') }} sell_token_price
        on sell_token_price.minute = date_trunc('minute', t.evt_block_time)
        and sell_token_price.contract_address = t.pay_gem
        and sell_token_price.blockchain = 'optimism'
        {% if not is_incremental() %}
        and sell_token_price.minute >= '{{ project_start_date }}'
        {% endif %}
    left join {{ source('prices', 'usd') }} buy_token_price
        on buy_token_price.minute = date_trunc('minute', t.evt_block_time)
        and buy_token_price.contract_address = t.buy_gem
        and buy_token_price.blockchain = 'optimism'
        {% if not is_incremental() %}
        and buy_token_price.minute >= '{{ project_start_date }}'
        {% endif %}
),

trades_grouped as 
(
    -- group the trades by offer id
    select 
        offer_id,
        sell_token_address,
        buy_token_address,
        sum(sold_amount_raw) as sold_amount_raw,
        sum(bought_amount_raw) as bought_amount_raw,
        sum(sold_amount) as sold_amount,
        sum(bought_amount) as bought_amount,
        sum(sold_amount_usd) as sold_amount_usd,
        sum(bought_amount_usd) as bought_amount_usd,
        sell_token_symbol,
        buy_token_symbol
    from trades
    group by offer_id, sell_token_address, buy_token_address, sell_token_symbol, buy_token_symbol
)

select 
    'optimism' AS blockchain,
    'rubicon' AS project,
    '1' AS version,
    TRY_CAST(date_trunc('DAY', offers.block_time) AS date) as block_date,
    offers.block_time,
    erc20_sell.symbol AS sell_token_symbol, -- TODO: add select for erc20_sell
    erc20_buy.symbol AS buy_token_symbol, -- TODO: add select for erc20_buy
    case 
        when lower(erc20_sell.symbol) > lower(erc20_buy.symbol) then concat(erc20_buy.symbol, '-', erc20_sell.symbol)
        else concat(erc20_sell.symbol, '-', erc20_buy.symbol)
    end as token_pair,
    offers.sell_amount_raw / power(10, erc20_sell.decimals) AS sell_amount,
    offers.buy_amount_raw / power(10, erc20_buy.decimals) AS buy_amount,
    offers.sell_amount_raw,
    offers.buy_amount_raw,
    trades.sold_amount as sold_amount,
    trades.bought_amount as bought_amount,
    trades.sold_amount_raw as sold_amount_raw,
    trades.bought_amount_raw as bought_amount_raw,
    offers.sell_amount_raw / power(10, erc20_sell.decimals) * sell_token_price.price AS sell_amount_usd,
    offers.buy_amount_raw / power(10, erc20_buy.decimals) * buy_token_price.price AS buy_amount_usd,
    trades.sold_amount_usd as sold_amount_usd,
    trades.bought_amount_usd as bought_amount_usd,
    txn.gas_price AS gas_price,
    txn.gas_used AS gas_used,
    txn.l1_gas_price AS l1_gas_price,
    txn.l1_gas_used AS l1_gas_used,
    txn.l1_fee_scalar AS l1_fee_scalar,
    ((CAST(txn.gas_used AS DECIMAL(38,0)) / power(10, 18)) * CAST(txn.gas_price as decimal(38,0))) + ((CAST(txn.l1_gas_used AS DECIMAL(38,0)) / power(10, 18)) * CAST(txn.l1_gas_price as decimal(38,0)) * CAST(txn.l1_fee_scalar as decimal(38,0))) as txn_cost_eth,
    eth.price as eth_price,
    ((CAST(txn.gas_used AS DECIMAL(38,0)) / power(10, 18)) * CAST(txn.gas_price as decimal(38,0))) + ((CAST(txn.l1_gas_used AS DECIMAL(38,0)) / power(10, 18)) * CAST(txn.l1_gas_price as decimal(38,0)) * CAST(txn.l1_fee_scalar as decimal(38,0))) * eth.price as txn_cost_usd,
from offers

inner join {{ source('optimism', 'transactions') }} txn
    on txn.hash = offers.tx_hash
    and txn.block_number = offers.block_number
    {% if not is_incremental() %}
    and txn.block_time >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    and txn.block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
left join {{ ref('tokens_erc20') }} erc20_sell
    on erc20_sell.contract_address = offers.sell_token_address
    and erc20_sell.blockchain = 'optimism'
left join {{ ref('tokens_erc20') }} erc20_buy
    on erc20_buy.contract_address = offers.buy_token_address
    and erc20_buy.blockchain = 'optimism'
left join {{ source('prices', 'usd') }} sell_token_price
    on sell_token_price.minute = date_trunc('minute', offers.block_time)
    and sell_token_price.contract_address = offers.sell_token_address
    and sell_token_price.blockchain = 'optimism'
    {% if not is_incremental() %}
    and sell_token_price.minute >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    and sell_token_price.minute >= date_trunc('day', now() - interval '1 week')
    {% endif %}
left join {{ source('prices', 'usd') }} buy_token_price
    on buy_token_price.minute = date_trunc('minute', offers.block_time)
    and buy_token_price.contract_address = offers.buy_token_address
    and buy_token_price.blockchain = 'optimism'
    {% if not is_incremental() %}
    and buy_token_price.minute >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    and buy_token_price.minute >= date_trunc('day', now() - interval '1 week')
    {% endif %}
left join {{ source('prices', 'usd') }} eth
    on eth.minute = date_trunc('minute', offers.block_time)
    and eth.contract_address = '0xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000' -- this is for optimism specifically
    and eth.blockchain = 'optimism'
    {% if not is_incremental() %}
    and eth.minute >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    and eth.minute >= date_trunc('day', now() - interval '1 week')
    {% endif %}
left join trades
    on trades.offer_id = offers.offer_id
    {% if not is_incremental() %}
    and trades.block_time >= '{{ project_start_date }}'
    {% endif %}
    {% if is_incremental() %}
    and trades.block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
;


    
