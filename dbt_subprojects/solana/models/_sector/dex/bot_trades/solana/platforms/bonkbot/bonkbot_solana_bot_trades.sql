{{ config(
    schema = 'bonkbot_solana',
    alias = 'bot_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'outer_instruction_index', 'inner_instruction_index']
   )
}}

{% set bot_label = 'BonkBot' %}
{% set project_start_date = '2023-08-17' %}
{% set fee_receiver = 'ZG98FUCjb8mJ824Gbs6RsgVmr1FhXb2oNiJHa2dwmPd' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}
with
    trades as (
        select
            '{{bot_label}}' as bot,
            'solana' as blockchain,
            trades.block_time,
            cast(date_trunc('day', trades.block_time) as date) as block_date,
            cast(date_trunc('month', trades.block_time) as date) as block_month,
            trades.amount_usd,
            -- TODO: find a more generic solution for this
            if(token_sold_mint_address = '{{wsol_token}}', 'Buy', 'Sell') as type,
            token_bought_amount,
            token_bought_symbol,
            token_bought_mint_address as token_bought_address,
            token_sold_amount,
            token_sold_symbol,
            token_sold_mint_address as token_sold_address,
            fee_payments.amount_usd as fee_usd,
            fee_payments.amount as fee_token_amount,
            fee_payments.token_symbol as fee_token_symbol,
            fee_payments.token_address as fee_token_address,
            project,
            trades.version,
            token_pair,
            project_program_id as project_contract_address,
            trader_id as user,
            trades.tx_id,
            tx_index,
            outer_instruction_index,
            inner_instruction_index
        from {{ source('dex_solana', 'trades') }} as trades
        join
            {{ ref('bonkbot_solana_fee_payments_usd') }} as fee_payments
            on (
                trades.tx_id = fee_payments.tx_id
                and fee_payments.block_time = trades.block_time
                and fee_payments.index = 1  -- only get the first fee payment per tx
                and trades.trader_id != fee_payments.fee_receiver
            )
        where
            trades.trader_id != '{{fee_receiver}}'  -- Exclude trades signed by FeeWallet
            -- TODO: find a efficient solution for this AND transactions.signer != '{{fee_receiver}}' -- Exclude trades signed by FeeWallet
            -- TODO: to filtering for signer in 2nd stage/cte
            {% if is_incremental() %}
                and {{ incremental_predicate('trades.block_time') }}
                and {{ incremental_predicate('fee_payments.block_time') }}
            {% else %}
                and trades.block_time >= timestamp '{{project_start_date}}'
                and fee_payments.block_time >= timestamp '{{project_start_date}}'
            {% endif %}
    ),
    highest_inner_instruction_index_for_each_trade as (
        select
            tx_id,
            outer_instruction_index,
            max(inner_instruction_index) as highest_inner_instruction_index
        from trades
        group by tx_id, outer_instruction_index
    )
select
    trades.*,
    if(
        inner_instruction_index = highest_inner_instruction_index, true, false
    ) as is_last_trade_in_transaction
from trades
join
    highest_inner_instruction_index_for_each_trade
    on (
        trades.tx_id = highest_inner_instruction_index_for_each_trade.tx_id
        and trades.outer_instruction_index
        = highest_inner_instruction_index_for_each_trade.outer_instruction_index
    )
order by
    block_time desc,
    tx_index desc,
    outer_instruction_index desc,
    inner_instruction_index desc
