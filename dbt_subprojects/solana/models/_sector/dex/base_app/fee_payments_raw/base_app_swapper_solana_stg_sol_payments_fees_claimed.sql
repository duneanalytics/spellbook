{{ config(
    schema = 'base_app_swapper_solana',
    alias = 'stg_sol_payments_fees_claimed',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'token_address']
   )
}}

{% set query_start_date = '2024-04-01' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

-- From '2024-04' to '2025-10 fees are claimed by 6Ro...TcyB
-- Claiming occurs in seperate transaction to the trades
select
    account_activity.block_time,
    cast(date_trunc('month', account_activity.block_time) as date) as block_month,
    address as fee_receiver,
    account_activity.balance_change / 1e9 as amount,
    '{{wsol_token}}' token_address,
    account_activity.tx_id
from {{ source('solana','account_activity') }} as account_activity
where
    {% if is_incremental() %} 
    {{ incremental_predicate('account_activity.block_time') }}
    {% else %} 
    account_activity.block_time >= timestamp '{{query_start_date}}'
    {% endif %} 
    and tx_success
    and address = '6RogbrW13c2MqdJBinNHGPucykeDPxbzYZGx1RuXTcyB'
    and balance_change > 0
    and exists (
        select 1 
        from {{ source('jupiter_solana', 'referral_call_claim') }} as claims
        where claims.call_tx_id = account_activity.tx_id
        and claims.call_block_time = account_activity.block_time
        and account_referralAccount = '6dKqNQ332RmucYf6cMHXfP4pcsi7wnY8XB87emdiM8QX'
        {% if is_incremental() %} 
        and {{ incremental_predicate('claims.call_block_time') }}
        {% else %} 
        and claims.call_block_time >= timestamp '{{query_start_date}}'
        {% endif %} 
    )