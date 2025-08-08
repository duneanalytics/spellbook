{{ config(
    schema = 'phantom_swapper_solana',
    alias = 'bot_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'block_slot', 'tx_index', 'outer_instruction_index', 'inner_instruction_index']
   )
}}

{% set query_start_date = '2024-01-01' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

with filtered_transactions as (
        select 
            id, 
            signer,
            block_date
        from {{ source('solana', 'transactions') }}
        where
            {% if is_incremental() %} 
                {{ incremental_predicate('block_date') }}
            {% else %} 
                block_date >= timestamp '{{query_start_date}}'
            {% endif %} 
    ),
    trades as (
        select
            'solana' as blockchain,
            trades.block_time,
            cast(date_trunc('day', trades.block_time) as date) as block_date,
            cast(date_trunc('month', trades.block_time) as date) as block_month,
            trades.amount_usd,
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
            trades.block_slot,
            tx_index,
            outer_instruction_index,
            inner_instruction_index
        from {{ source('dex_solana', 'trades') }} as trades
        join
            {{ ref('phantom_swapper_solana_fee_payments_usd') }} as fee_payments
            on (
                trades.tx_id = fee_payments.tx_id
                and fee_payments.block_time = trades.block_time
                and fee_payments.index = 1  -- only get the first fee payment per tx
                and trades.trader_id != fee_payments.fee_receiver
            )
        join filtered_transactions as tx ON trades.tx_id = tx.id 
        left join {{ ref("phantom_swapper_solana_fee_addresses") }} as fa1 ON fa1.fee_receiver = trades.trader_id
        left join {{ ref("phantom_swapper_solana_fee_addresses") }} as fa2 ON fa2.fee_receiver = tx.signer
        where
            fa1.fee_receiver IS NULL -- Exclude trades where FeeWallet is trader
            and fa2.fee_receiver IS NULL -- Exclude transactions signed by FeeWallet 
            and trades.trade_source IN ('JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4','6m2CDdhRgxpH4WjvdzxAYbGxwdGUz5MziiL5jek2kBma', 'JUPSjgjMFjU4453KMgxhqVmzep6W352bQpE4RsNqXAx')
            {% if is_incremental() %}
                and {{ incremental_predicate('trades.block_time') }}
                and {{ incremental_predicate('fee_payments.block_time') }}
            {% else %}
                and trades.block_time >= timestamp '{{query_start_date}}'
                and fee_payments.block_time >= timestamp '{{query_start_date}}'
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
