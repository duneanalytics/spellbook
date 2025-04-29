{{
    config(
        tags = ['prod_exclude'],
        alias='bot_trades',
        schema='flokibot_bnb',
        partition_by=['block_month'],
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        incremental_predicates=[
            incremental_predicate('DBT_INTERNAL_DEST.block_time')
        ],
        unique_key=['blockchain', 'tx_hash', 'evt_index'],
    )
}}

{% set project_name = 'Floki Trading Bot' %}
{% set project_start_date = '2024-02-13' %}
{% set blockchain = 'bnb' %}
{% set bot_deployer_1 = '0xdeb9E55E0F20bC59029271372ECea50E67182A3A' %}
{% set bot_deployer_2 = '0xfE188fee982CF9588841ea8540bD490a495D9f2c' %}
{% set bot_deployer_3 = '0xcE6a13955EC32B6B1b7EBe089302b536Ad40aeC3' %}
{% set treasury_fee_wallet_1 = '0xc69df57dbb39e52d5836753e6abb71a9ab271c2d' %}
{% set treasury_fee_wallet_2 = '0x07b127f66cf580aBd2FC59b7836fc2CF6cec3502' %}
{% set treasury_fee_wallet_3 = '0x197d8fab9d5a4c026a56d01f529caf023ba46df5' %}
{% set buyback_fee_wallet_1 = '0xCc5374Be204990A3205EB9f93C5bD37B4f8e2c5e' %}
{% set aggregator_fee_wallet_1 = '0x7b41114eCB5C09d483343116C229Be3d3eb3b0fC' %}
{% set wbnb = '0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c' %}
{% set fee_token_symbol = 'BNB' %}

with
    bot_contracts as (
        select block_time, address
        from {{ source('bnb', 'creation_traces') }}
        where
            ("from" = {{ bot_deployer_1 }} or "from" = {{ bot_deployer_2 }} or "from" = {{ bot_deployer_3 }})
            and block_time >= timestamp '{{project_start_date}}'
    ),
    fees as (
      select sum(value) / 1e18 as fee_token_amount, tx_hash
        from {{ source('bnb', 'traces') }}
        where
         (
                to = {{ aggregator_fee_wallet_1 }}
                or to = {{ treasury_fee_wallet_1 }}
                or to = {{ treasury_fee_wallet_2 }}
                or to = {{ treasury_fee_wallet_3 }}
                or to = {{ buyback_fee_wallet_1 }}
            )
            and tx_success = true
            and value > 0
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            {% else %} and block_time >= timestamp '{{project_start_date}}'
            {% endif %}
        group by tx_hash
    ),
    oneinch_aggregator_trades as (
        select call_block_time as block_time, call_tx_hash as tx_hash
        from {{ source('oneinch_bnb', 'AggregationRouterV6_call_swap') }}
        where
            (
                varbinary_position(data, {{ aggregator_fee_wallet_1 }}) > 0
                or varbinary_position(data, {{ treasury_fee_wallet_1 }}) > 0
                or varbinary_position(data, {{ treasury_fee_wallet_2 }}) > 0
                or varbinary_position(data, {{ treasury_fee_wallet_3 }}) > 0
                or varbinary_position(data, {{ buyback_fee_wallet_1 }}) > 0
            )
            and call_success
            {% if is_incremental() %}
                and {{ incremental_predicate('call_block_time') }}
            {% else %} and call_block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    ),
    openocean_aggregator_trades as (
        select evt_block_time as block_time, evt_tx_hash as tx_hash
        from {{ source('openocean_v2_bnb', 'OpenOceanExchange_evt_Swapped') }}
        where
            referrer = {{ aggregator_fee_wallet_1 }}
            OR referrer = {{ treasury_fee_wallet_1 }}
            OR referrer = {{ treasury_fee_wallet_2 }}
            OR referrer = {{ treasury_fee_wallet_3 }}
            OR referrer = {{ buyback_fee_wallet_1 }}
            {% if is_incremental() %}
                and {{ incremental_predicate('evt_block_time') }}
            {% else %} and evt_block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    ),
    trade_transactions as (
        select block_time, address, null as tx_hash
        from bot_contracts
        union all
        select block_time, null as address, tx_hash
        from oneinch_aggregator_trades
        union all
        select block_time, null as address, tx_hash
        from openocean_aggregator_trades
    ),
    bot_trades as (
        select
            trades.block_time,
            trades.block_number,
            amount_usd,
            if(token_sold_address = {{ wbnb }}, 'Buy', 'Sell') as type,
            token_bought_amount,
            token_bought_symbol,
            token_bought_address,
            token_sold_amount,
            token_sold_symbol,
            token_sold_address,
            fee_token_amount,
            '{{fee_token_symbol}}' as fee_token_symbol,
            {{ wbnb }} as fee_token_address,
            project,
            version,
            token_pair,
            project_contract_address,
            tx_from as user,
            tx_to as bot,
            trades.tx_hash,
            evt_index
        from {{ source('dex', 'trades') }} as trades
        join trade_transactions ON (
          (
            trades.tx_to = trade_transactions.address
            OR trades.tx_hash = trade_transactions.tx_hash
          )
          AND trades.block_time >= trade_transactions.block_time
        )
        left join fees on fees.tx_hash = trades.tx_hash
        where
            trades.blockchain = '{{blockchain}}'
            {% if is_incremental() %}
                and {{ incremental_predicate('trades.block_time') }}
            {% else %} and trades.block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    ),
    highest_event_index_for_each_trade as (
        select tx_hash, max(evt_index) as highest_event_index
        from bot_trades
        group by tx_hash
    )
select
    block_time,
    date_trunc('day', bot_trades.block_time) as block_date,
    cast(date_trunc('month', bot_trades.block_time) as date) as block_month,
    '{{project_name}}' as bot,
    block_number,
    '{{blockchain}}' as blockchain,
    -- Trade
    amount_usd,
    type,
    token_bought_amount,
    token_bought_symbol,
    token_bought_address,
    token_sold_amount,
    token_sold_symbol,
    token_sold_address,
    -- Fees
    round(
        fee_token_amount * price / cast(amount_usd as double),
        4 -- Round feePercentage to 0.01% steps
    ) as fee_percentage_fraction,
    fee_token_amount * price as fee_usd,
    fee_token_amount,
    '{{fee_token_symbol}}' as fee_token_symbol,
    fee_token_address,
    -- Dex
    project,
    version,
    token_pair,
    project_contract_address,
    -- User
    user as user,
    bot_trades.tx_hash,
    evt_index,
    if(evt_index = highest_event_index, true, false) as is_last_trade_in_transaction
from bot_trades
join
    highest_event_index_for_each_trade
    on bot_trades.tx_hash = highest_event_index_for_each_trade.tx_hash
left join
    {{ source('prices', 'usd') }}
    on (
        blockchain = '{{blockchain}}'
        and contract_address = {{ wbnb }}
        and minute = date_trunc('minute', block_time)
        {% if is_incremental() %} and {{ incremental_predicate('minute') }} {% endif %}
    )
order by block_time desc, evt_index desc
