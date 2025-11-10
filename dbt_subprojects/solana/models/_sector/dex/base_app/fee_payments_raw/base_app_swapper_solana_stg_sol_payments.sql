{{ config(
    schema = 'base_app_swapper_solana',
    alias = 'stg_sol_payments',
    partition_by = ['block_month'],
    materialized = 'incremental',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'token_address']
   )
}}

with sol_payments as (
    select
        block_time,
        block_month,
        fee_receiver,
        amount,
        token_address,
        tx_id
    from {{ ref('base_app_swapper_solana_stg_sol_payments_fees_paid') }}
    where
        {% if is_incremental() or true %} 
        {{ incremental_predicate('block_time') }}
        {% endif %}
    union all
    select
        block_time,
        block_month,
        fee_receiver,
        amount,
        token_address,
        tx_id
    from {{ ref('base_app_swapper_solana_stg_sol_payments_fees_claimed') }}
    where
        {% if is_incremental() or true %} 
        {{ incremental_predicate('block_time') }}
        {% endif %}
)
select *
from sol_payments