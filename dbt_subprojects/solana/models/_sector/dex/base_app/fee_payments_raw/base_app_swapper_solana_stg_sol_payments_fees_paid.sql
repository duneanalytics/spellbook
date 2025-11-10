{{ config(
    schema = 'base_app_swapper_solana',
    alias = 'stg_sol_payments_fees_paid',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'token_address']
   )
}}

{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

-- From '2025-10' onwards fees are paid directly to 6oo...yBd during each trade
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
    account_activity.block_time >= timestamp '2025-10-16'
    {% endif %} 
    and tx_success
    and address = '6ooVBXhnqAXaF91cu49YmWhoFuE6WLdZWTwNYvTuhyBd'
    and balance_change > 0
    and exists (
        select 1 
        from {{ source('dex_solana', 'trades') }} as trades
        where trades.tx_id = account_activity.tx_id
        and trades.block_time = account_activity.block_time
        and trades.block_month = cast(date_trunc('month', account_activity.block_time) as date)
        {% if is_incremental() %} 
        and {{ incremental_predicate('trades.block_time') }}
        {% else %} 
        and trades.block_time >= timestamp '2025-10-16'
        {% endif %} 
    )