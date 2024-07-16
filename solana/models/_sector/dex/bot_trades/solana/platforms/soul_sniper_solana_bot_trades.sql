{{
    config(
        alias='bot_trades',
        schema='soul_sniper_solana',
        partition_by=['block_month'],
        materialized='incremental',
        file_format='delta',
        incremental_strategy='merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key=[
            'blockchain',
            'tx_id',
            'tx_index',
            'outer_instruction_index',
            'inner_instruction_index',
        ]
    )
}}

{% set project_start_date = '2024-01-06' %}
{% set fee_receiver = '6MgcmcZJXfyux7rRSsvjWaG8iegQ29KLxMtWHKoCZ7fn' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

with
    all_fee_payments as (
        select * from {{ref('soul_sniper_solana_bot_users')}}
    ),
    bot_trades as (
        select
            trades.block_time,
            cast(date_trunc('day', trades.block_time) as date) as block_date,
            cast(date_trunc('month', trades.block_time) as date) as block_month,
            'solana' as blockchain,
            amount_usd,
            if(token_sold_mint_address = '{{wsol_token}}', 'Buy', 'Sell') as type,
            token_bought_amount,
            token_bought_symbol,
            token_bought_mint_address as token_bought_address,
            token_sold_amount,
            token_sold_symbol,
            token_sold_mint_address as token_sold_address,
            0 as fee_usd,
            0 AS fee_token_amount,
            'SOL' as fee_token_symbol,
            '{{wsol_token}}' as fee_token_address,
            project,
            version,
            token_pair,
            project_program_id as project_contract_address,
            trader_id as user,
            trades.tx_id,
            tx_index,
            outer_instruction_index,
            inner_instruction_index
        from {{ ref('dex_solana_trades') }} as trades
        join all_fee_payments on trades.trader_id = all_fee_payments.user
        join
            {{ source('solana', 'transactions') }} as transactions
            on (
                trades.block_time = transactions.block_time
                and trades.tx_id = id
                {% if is_incremental() %}
                    and {{ incremental_predicate('transactions.block_time') }}
                    and {{ incremental_predicate('trades.block_time') }}
                {% else %}
                    and transactions.block_time >= timestamp '{{project_start_date}}'
                    and trades.block_time >= timestamp '{{project_start_date}}'
                {% endif %}
            )
        where
            trades.trader_id != '{{fee_receiver}}'  -- Exclude trades signed by FeeWallet
            and transactions.signer != '{{fee_receiver}}'  -- Exclude trades signed by FeeWallet
    ),
    highest_inner_instruction_index_for_each_trade as (
        select
            tx_id,
            outer_instruction_index,
            max(inner_instruction_index) as highest_inner_instruction_index
        from bot_trades
        group by tx_id, outer_instruction_index
    )
select
    block_time,
    block_date,
    block_month,
    'Soul Sniper' as bot,
    blockchain,
    amount_usd,
    type,
    token_bought_amount,
    token_bought_symbol,
    token_bought_address,
    token_sold_amount,
    token_sold_symbol,
    token_sold_address,
    fee_usd,
    fee_token_amount,
    fee_token_symbol,
    fee_token_address,
    project,
    version,
    token_pair,
    project_contract_address,
    user,
    bot_trades.tx_id,
    tx_index,
    bot_trades.outer_instruction_index,
    coalesce(inner_instruction_index, 0) as inner_instruction_index,
    if(
        inner_instruction_index = highest_inner_instruction_index, true, false
    ) as is_last_trade_in_transaction
from bot_trades
join
    highest_inner_instruction_index_for_each_trade
    on (
        bot_trades.tx_id = highest_inner_instruction_index_for_each_trade.tx_id
        and bot_trades.outer_instruction_index
        = highest_inner_instruction_index_for_each_trade.outer_instruction_index
    )
order by
    block_time desc,
    tx_index desc,
    outer_instruction_index desc,
    inner_instruction_index desc
