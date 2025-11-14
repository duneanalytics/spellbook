{{ config(
    schema = 'base_app_swapper_solana',
    alias = 'stg_token_payments_fees_claimed',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'token_address']
   )
}}

{% set query_start_date = '2024-04-01' %}

-- From '2024-04' to '2025-10 fees are claimed by 6Ro...TcyB        
select
    account_activity.block_time,
    cast(date_trunc('month', account_activity.block_time) as date) as block_month,
    token_balance_owner as fee_receiver,
    account_activity.token_balance_change as amount,
    account_activity.token_mint_address as token_address,
    account_activity.tx_id
from {{ source('solana','account_activity') }} as account_activity
where
    {% if is_incremental() %} 
    {{ incremental_predicate('account_activity.block_time') }}
    {% else %} 
    account_activity.block_time >= timestamp '{{query_start_date}}'
    {% endif %} 
    and token_balance_owner = '6RogbrW13c2MqdJBinNHGPucykeDPxbzYZGx1RuXTcyB'
    and token_balance_change > 0
    and tx_success
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