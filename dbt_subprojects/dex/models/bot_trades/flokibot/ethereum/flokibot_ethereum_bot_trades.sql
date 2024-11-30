{{
    config(
        alias='bot_trades',
        schema='flokibot_ethereum',
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
{% set project_start_date = '2024-02-01' %}
{% set blockchain = 'ethereum' %}
{% set bot_deployer_1 = '0xdeb9E55E0F20bC59029271372ECea50E67182A3A' %}
{% set bot_deployer_2 = '0xcE6a13955EC32B6B1b7EBe089302b536Ad40aeC3' %}
{% set treasury_fee_wallet_1 = '0xc69df57dbb39e52d5836753e6abb71a9ab271c2d' %}
{% set treasury_fee_wallet_2 = '0xffdc626bb733a8c2e906242598e2e99752dcb922' %}
{% set aggregator_fee_wallet_2 = '0x7b41114eCB5C09d483343116C229Be3d3eb3b0fC' %}
{% set weth = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
{% set fee_token_symbol = 'ETH' %}

with
    bot_contracts as (
        select address
        from {{ source('ethereum', 'creation_traces') }}
        where
            ("from" = {{ bot_deployer_1 }} or "from" = {{ bot_deployer_2 }})
            and block_time >= timestamp '{{project_start_date}}'
    ),
    bot_trades as (
        select
            trades.block_time,
            amount_usd,
            if(token_sold_address = {{ weth }}, 'Buy', 'Sell') as type,
            token_bought_amount,
            token_bought_symbol,
            token_bought_address,
            token_sold_amount,
            token_sold_symbol,
            token_sold_address,
            project,
            version,
            token_pair,
            project_contract_address,
            tx_from as user,
            tx_to as bot,
            trades.tx_hash,
            evt_index
        from {{ source('dex', 'trades') }} as trades
        join bot_contracts on trades.tx_to = bot_contracts.address
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
    ),
    treasury_fees as (
        select value / 1e18 as treasury_fee, tx_hash
        from ethereum.traces
        where
            (
                to = {{ treasury_fee_wallet_1 }}
                or to = {{ treasury_fee_wallet_2 }}
            )
            and tx_success
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            {% else %} and block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    ),
    buyback_fees as (
        select value / 1e18 as buyback_fee, tx_hash
        from ethereum.traces
        where
            to = {{ bubyack_fee_wallet_1 }}
            and tx_success = true
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            {% else %} and block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    ),
    oneinch_aggregator_trades as (
        select call_block_time as block_time, call_tx_hash as tx_hash
        from {{ source('oneinch_ethereum', 'AggregationRouterV6_call_swap') }}
        where
            (
                varbinary_position(data, {{ aggregator_fee_wallet_2 }}) > 0
                or varbinary_position(data, {{ treasury_fee_wallet_2 }}) > 0
            )
            and call_success
            {% if is_incremental() %}
                and {{ incremental_predicate('call_block_time') }}
            {% else %} and call_block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    ),
    bot_eth_deposits as (
        select
            tx_hash,
            block_number,
            cast(value as decimal(38, 0)) as delta_gwei,
            cast(value as decimal(38, 0)) as deposit_gwei
        from {{ source('ethereum', 'traces') }}
        join bot_contracts on to = bot_contracts.address
        where
            {% if is_incremental() %} {{ incremental_predicate('block_time') }}
            {% else %} block_time >= timestamp '{{project_start_date}}'
            {% endif %} and value > 0
    ),
    bot_eth_withdrawals as (
        select
            tx_hash,
            block_number,
            cast(value as decimal(38, 0)) * -1 as delta_gwei,
            0 as deposit_gwei,
            block_hash,
            to
        from {{ source('ethereum', 'traces') }}
        join bot_contracts on "from" = bot_contracts.address
        where
            {% if is_incremental() %} {{ incremental_predicate('block_time') }}
            {% else %} block_time >= timestamp '{{project_start_date}}'
            {% endif %} and value > 0
    ),
    botethtransfers as (
        /* Deposits */
        (select tx_hash, block_number, delta_gwei, deposit_gwei from bot_eth_deposits)
        union all
        /* Withdrawals */
        (
            select tx_hash, block_number, delta_gwei, deposit_gwei
            from bot_eth_withdrawals
        )
    ),
    bot_eth_deltas as (
        select
            tx_hash,
            block_number,
            sum(delta_gwei) as fee_gwei,
            sum(deposit_gwei) as deposit_gwei
        from botethtransfers
        group by tx_hash, block_number
    )
select
    block_time,
    date_trunc('day', bot_trades.block_time) as block_date,
    date_trunc('month', bot_trades.block_time) as block_month,
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
        cast(fee_gwei as double) / cast(deposit_gwei as double),
        4 -- Round feePercentage to 0.01% steps
    ) as fee_percentage_fraction,
    (fee_gwei / 1e18) * price as fee_usd,
    fee_gwei / 1e18 fee_token_amount,
    '{{fee_token_symbol}}' as fee_token_symbol,
    {{ weth }} as fee_token_address,
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
left join bot_eth_deltas on bot_trades.tx_hash = bot_eth_deltas.tx_hash
left join
    {{ source('prices', 'usd') }}
    on (
        blockchain = '{{blockchain}}'
        and contract_address = {{ weth }}
        and minute = date_trunc('minute', block_time)
        {% if is_incremental() %} and {{ incremental_predicate('minute') }} {% endif %}
    )
order by block_time desc, evt_index desc
