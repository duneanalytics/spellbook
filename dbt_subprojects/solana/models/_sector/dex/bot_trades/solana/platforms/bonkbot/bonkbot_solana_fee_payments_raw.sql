{{ config(
    schema = 'bonkbot_solana',
    alias = 'fee_payments_raw',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'token_address']
   )
}}

{% set bot_label = 'BonkBot' %}
{% set blockchain = 'solana' %}
{% set project_start_date = '2023-08-17' %}
{% set fee_receiver = 'ZG98FUCjb8mJ824Gbs6RsgVmr1FhXb2oNiJHa2dwmPd' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

with
    fee_addresses as (select '{{fee_receiver}}' as fee_receiver),
    fee_payments as (
        select
            block_time,
            cast(date_trunc('month', block_time) as date) as block_month,
            fee_receiver,
            if(
                balance_change > 0, balance_change / 1e9, token_balance_change
            ) as amount,
            if(
                balance_change > 0, '{{wsol_token}}', token_mint_address
            ) as token_address,
            tx_id
        from {{ source('solana','account_activity') }} as account_activity
        join
            fee_addresses
            on (
                (
                    fee_addresses.fee_receiver = account_activity.address
                    and balance_change > 0
                )
                or (
                    token_balance_owner = fee_addresses.fee_receiver
                    and token_balance_change > 0
                )
            )
        where
            {% if is_incremental() %} {{ incremental_predicate('block_time') }}
            {% else %} block_time >= timestamp '{{project_start_date}}'
            {% endif %} and tx_success
    ),
    -- Eliminate duplicates (e.g. both SOL + WSOL in a single transaction)
    aggregated_fee_payments_by_token_by_tx as (
        select
            block_time,
            block_month,
            token_address,
            fee_receiver,
            tx_id,
            sum(amount) as amount
        from fee_payments
        group by tx_id, token_address, fee_receiver, block_time, block_month
    )
select
    block_time,
    block_month,
    '{{bot_label}}' as bot,
    '{{blockchain}}' as blockchain,
    amount,
    token_address,
    fee_receiver,
    tx_id,
    row_number() over (
        partition by tx_id
        order by
            case when token_address = '{{wsol_token}}' then 0 else 1 end,
            token_address asc
    ) as index
from aggregated_fee_payments_by_token_by_tx
