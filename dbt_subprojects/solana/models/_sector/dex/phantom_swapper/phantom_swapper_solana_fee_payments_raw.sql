{{ config(
    schema = 'phantom_swapper_solana',
    alias = 'fee_payments_raw',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'token_address']
   )
}}

{% set query_start_date = '2024-01-01' %}
{% set blockchain = 'solana' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

-- Single pass over account_activity: one row can carry both a SOL and a token
-- payment, so each qualifying row is split into up to two payment legs instead
-- of scanning account_activity (and re-probing trades) once per leg type.
with swap_account_activity as (
        select
            account_activity.block_time,
            account_activity.tx_id,
            account_activity.address,
            account_activity.balance_change,
            account_activity.token_balance_owner,
            account_activity.token_balance_change,
            account_activity.token_mint_address
        from {{ source('solana','account_activity') }} as account_activity
        where
            {% if is_incremental() %} 
                {{ incremental_predicate('account_activity.block_time') }}
            {% else %} 
                account_activity.block_time >= timestamp '{{query_start_date}}'
            {% endif %} 
            and tx_success
            and (balance_change > 0 or token_balance_change > 0)
            and exists (
                select 1 
                from {{ source('dex_solana', 'trades') }} as trades
                where trades.tx_id = account_activity.tx_id
                and trades.block_time = account_activity.block_time
                {% if is_incremental() %} 
                and {{ incremental_predicate('trades.block_time') }}
                {% else %} 
                and trades.block_time >= timestamp '{{query_start_date}}'
                {% endif %} 
            )
    ),
    payment_legs as (
        select
            activity.block_time,
            cast(date_trunc('month', activity.block_time) as date) as block_month,
            l.receiver_address,
            l.amount,
            l.token_address,
            activity.tx_id
        from swap_account_activity as activity
        cross join unnest(array[
            case when activity.balance_change > 0 then
                cast(row(activity.address, activity.balance_change / 1e9, '{{wsol_token}}')
                    as row(receiver_address varchar, amount double, token_address varchar))
            end,
            case when activity.token_balance_change > 0 then
                cast(row(activity.token_balance_owner, cast(activity.token_balance_change as double), activity.token_mint_address)
                    as row(receiver_address varchar, amount double, token_address varchar))
            end
        ]) as l(receiver_address, amount, token_address)
        where l.receiver_address is not null
    ),
    fee_payments as (
        select
            payment_legs.block_time,
            payment_legs.block_month,
            fee_addresses.fee_receiver,
            payment_legs.amount,
            payment_legs.token_address,
            payment_legs.tx_id
        from payment_legs
        join
            {{ ref("phantom_swapper_solana_fee_addresses") }} as fee_addresses
            on fee_addresses.fee_receiver = payment_legs.receiver_address
    ),
    filtered_transactions as (
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
    -- Eliminate duplicates (e.g. both SOL + WSOL in a single transaction)
    aggregated_fee_payments_by_token_by_tx as (
        select
            fee_payments.block_time,
            fee_payments.block_month,
            fee_payments.token_address,
            fee_payments.fee_receiver,
            fee_payments.tx_id,
            sum(fee_payments.amount) as amount
        from fee_payments
        join filtered_transactions tx 
        ON fee_payments.tx_id = tx.id 
        AND tx.signer != fee_payments.fee_receiver
        AND tx.block_date = date_trunc('day', fee_payments.block_time)
        group by 1,2,3,4,5
    )
select
    block_time,
    block_month,
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
