{{ config(
    alias = 'bot_trades',
    schema = 'banana_gun_solana',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = [
        'block_month',
        'blockchain',
        'tx_id',
        'tx_index',
        'outer_instruction_index',
        'inner_instruction_index'
    ]
   )
}}

{% set project_start_date = '2024-01-08' %}
{% set fee_receiver_1 = '8r2hZoDfk5hDWJ1sDujAi2Qr45ZyZw5EQxAXiMZWLKh2' %}
{% set fee_receiver_2 = 'Cj297UauzMX64FU9dKJZRUBWszJ7tEWpVheasq4CfATV' %}
{% set fee_receiver_3 = 'HKMh8nV3ysSofRi23LsfVGLGQKB415QAEfZT96kCcVj4' %}
{% set fee_receiver_4 = '7tQiiBdKoScWQkB1RmVuML7DBGnR31cuKPEtMM7Vy5SA' %}
{% set fee_receiver_5 = '4BBNEVRgrxVKv9f7pMNE788XM1tt379X9vNjpDH2KCL7' %}
{% set fee_receiver_6 = '47hEzz83VFR23rLTEeVm9A7eFzjJwjvdupPPmX3cePqF' %}
{% set fee_receiver_7 = 'EMbqD9Y9jLXEa3RbCR8AsEW1kVa3EiJgDLVgvKh4qNFP' %}
{% set fee_receiver_8 = 'Lk693UiTzQC4vobasRS1QGcYA9D6RGYLjHp1bWreQtM' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

with bot_trades as (
    select
        trades.block_time,
        cast(date_trunc('day', trades.block_time) as date) as block_date,
        cast(date_trunc('month', trades.block_time) as date) as block_month,
        'solana' as blockchain,
        trades.amount_usd,
        if(
            trades.token_sold_mint_address = '{{ wsol_token }}',
            'Buy',
            'Sell'
        ) as type,
        trades.token_bought_amount,
        trades.token_bought_symbol,
        trades.token_bought_mint_address as token_bought_address,
        trades.token_sold_amount,
        trades.token_sold_symbol,
        trades.token_sold_mint_address as token_sold_address,
        fee_payments.amount_usd as fee_usd,
        fee_payments.amount as fee_token_amount,
        'SOL' as fee_token_symbol,
        '{{ wsol_token }}' as fee_token_address,
        trades.project,
        trades.version,
        trades.token_pair,
        trades.project_program_id as project_contract_address,
        trades.trader_id as user,
        trades.tx_id,
        trades.tx_index,
        trades.outer_instruction_index,
        trades.inner_instruction_index
    from {{ source('dex_solana', 'trades') }} as trades
    join {{ ref('banana_gun_solana_fee_payments_usd') }} as fee_payments
        on trades.tx_id = fee_payments.tx_id
        and trades.block_time = fee_payments.block_time
        {% if is_incremental() %}
            and {{ incremental_predicate('fee_payments.block_time') }}
        {% else %}
            and fee_payments.block_time >= timestamp '{{ project_start_date }}'
        {% endif %}
    join {{ source('solana', 'transactions') }} as transactions
        on trades.tx_id = transactions.id
        and trades.block_time = transactions.block_time
        {% if is_incremental() %}
            and {{ incremental_predicate('transactions.block_time') }}
        {% else %}
            and transactions.block_time >= timestamp '{{ project_start_date }}'
        {% endif %}
    where
        trades.trader_id not in (
            '{{ fee_receiver_1 }}',
            '{{ fee_receiver_2 }}',
            '{{ fee_receiver_3 }}',
            '{{ fee_receiver_4 }}',
            '{{ fee_receiver_5 }}',
            '{{ fee_receiver_6 }}',
            '{{ fee_receiver_7 }}',
            '{{ fee_receiver_8 }}'
        )
        and transactions.signer not in (
            '{{ fee_receiver_1 }}',
            '{{ fee_receiver_2 }}',
            '{{ fee_receiver_3 }}',
            '{{ fee_receiver_4 }}',
            '{{ fee_receiver_5 }}',
            '{{ fee_receiver_6 }}',
            '{{ fee_receiver_7 }}',
            '{{ fee_receiver_8 }}'
        )
        {% if is_incremental() %}
            and {{ incremental_predicate('trades.block_time') }}
        {% else %}
            and trades.block_time >= timestamp '{{ project_start_date }}'
        {% endif %}
)

select
    block_time,
    block_date,
    block_month,
    'Banana Gun' as bot,
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
    tx_id,
    tx_index,
    outer_instruction_index,
    coalesce(inner_instruction_index, 0) as inner_instruction_index,
    if(
        inner_instruction_index = max(inner_instruction_index) over (
            partition by tx_id, outer_instruction_index
        ),
        true,
        false
    ) as is_last_trade_in_transaction
from bot_trades
