{{
    config(
        alias='bot_trades',
        schema='readyswap_ethereum',
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

{% set project_name = 'ReadySwap' %}
{% set project_start_date = '2023-05-12' %}
{% set blockchain = 'ethereum' %}
{% set bot_deployer_1 = '0xCf695491Dd1Afff04C50892dE0d758641e6D7Afd' %}
{% set fee_wallet_1 = '0x3d91e131Cc353018B4e95b7A5a475e8681fa6790' %}
{% set weth = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' %}
{% set fee_token_symbol = 'ETH' %}

with
    bot_contracts as (
        select address
        from {{ source('ethereum', 'creation_traces') }}
        where
            ("from" = {{ bot_deployer_1 }})
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
    fee_payments as (
        select
            tx_hash,
            block_number,
            sum(value) as fee_gwei
        from {{ source('ethereum', 'traces') }}
        where
            to = {{ fee_wallet_1 }}
            and value > 0
            {% if is_incremental() %} and {{ incremental_predicate('block_time') }}
            {% else %} and block_time >= timestamp '{{project_start_date}}'
            {% endif %} 
        group by tx_hash, block_number
    ),
    bot_eth_deposits as (
        select
            tx_hash,
            block_number,
            sum(value) as deposit_gwei
        from {{ source('ethereum', 'traces') }}
        join bot_contracts on to = bot_contracts.address
        where
            {% if is_incremental() %} {{ incremental_predicate('block_time') }}
            {% else %} block_time >= timestamp '{{project_start_date}}'
            {% endif %} 
            and value > 0
        group by tx_hash, block_number
    ),
    bot_deposits_and_fee_payments as (
        select 
            coalesce(bot_eth_deposits.tx_hash, fee_payments.tx_hash) as tx_hash,
            coalesce(bot_eth_deposits.block_number, fee_payments.block_number) as block_number,
            coalesce(fee_gwei, cast(0 AS UINT256)) as fee_gwei,
            coalesce(deposit_gwei, cast(0 AS UINT256)) as deposit_gwei
        from fee_payments
        full outer join bot_eth_deposits on fee_payments.tx_hash = bot_eth_deposits.tx_hash
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
left join bot_deposits_and_fee_payments on bot_trades.tx_hash = bot_deposits_and_fee_payments.tx_hash
left join
    {{ source('prices', 'usd') }}
    on (
        blockchain = '{{blockchain}}'
        and contract_address = {{ weth }}
        and minute = date_trunc('minute', block_time)
        {% if is_incremental() %} and {{ incremental_predicate('minute') }} {% endif %}
    )
order by block_time desc, evt_index desc
