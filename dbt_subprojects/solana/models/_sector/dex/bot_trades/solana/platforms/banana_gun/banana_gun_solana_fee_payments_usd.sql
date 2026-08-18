{{ config(
    schema = 'banana_gun_solana',
    alias = 'fee_payments_usd',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_month', 'tx_id', 'fee_receiver']
   )
}}

{% set project_start_date = '2024-01-08' %}
{% set ci_start_date = '2026-08-11' %}

with fee_payments as (
    select *
    from {{ ref('banana_gun_solana_fee_payments_raw') }}
    {% if is_incremental() %}
        where {{ incremental_predicate('block_time') }}
    {% else %}
        {# Temporary 7-day CI window; restore project_start_date for full-history validation. #}
        where block_time >= timestamp '{{ ci_start_date }}'
    {% endif %}
),

fee_token_prices as (
    select *
    from {{ ref('banana_gun_solana_fee_token_prices') }}
    {% if is_incremental() %}
        where {{ incremental_predicate('minute') }}
    {% else %}
        where minute >= timestamp '{{ project_start_date }}'
    {% endif %}
)

select
    fee_payments.block_time,
    fee_payments.block_month,
    fee_payments.bot,
    fee_payments.blockchain,
    fee_payments.amount,
    fee_token_prices.price as token_price_usd,
    fee_payments.amount * fee_token_prices.price as amount_usd,
    fee_token_prices.symbol as token_symbol,
    fee_token_prices.decimals as token_decimals,
    fee_payments.token_address,
    fee_payments.fee_receiver,
    fee_payments.fee_receiver_signed,
    fee_payments.tx_id
from fee_payments
left join fee_token_prices
    on fee_token_prices.contract_address_base58 = fee_payments.token_address
    and fee_token_prices.minute = date_trunc('minute', fee_payments.block_time)
