{{ config(
    schema = 'base_app_swapper_solana',
    alias = 'fee_payments_raw',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'token_address']
   )
}}

{% set query_start_date = '2024-04-01' %}
{% set blockchain = 'solana' %}
{% set wsol_token = 'So11111111111111111111111111111111111111112' %}

with sol_payments as (
    select
        block_time,
        block_month,
        fee_receiver,
        amount,
        token_address,
        tx_id
    from {{ ref('base_app_swapper_solana_stg_sol_payments') }}
    where
        1 = 1
        {% if is_incremental() %} 
        and {{ incremental_predicate('block_time') }}
        {% endif %}
),
token_payments as (
    select
        block_time,
        block_month,
        fee_receiver,
        amount,
        token_address,
        tx_id
    from {{ ref('base_app_swapper_solana_stg_token_payments') }}
    where
        1 = 1
        {% if is_incremental() %} 
        and {{ incremental_predicate('block_time') }}
        {% endif %}  
),
fee_payments as (
        select *
        from sol_payments
        union all
        select *
        from token_payments
),
filtered_transactions as (
        select 
            id, 
            signer,
            block_date
        from {{ source('solana', 'transactions') }}
        where
            1 = 1
            {% if is_incremental() %} 
            and {{ incremental_predicate('block_date') }}
            {% else %} 
            and block_date >= timestamp '{{query_start_date}}'
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
        AND tx.block_date = cast(date_trunc('day', fee_payments.block_time) as date)
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