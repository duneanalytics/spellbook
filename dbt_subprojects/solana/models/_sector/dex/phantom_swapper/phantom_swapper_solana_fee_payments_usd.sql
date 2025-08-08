{{
    config(
        schema="phantom_swapper_solana",
        alias="fee_payments_usd",
        partition_by=["block_month"],
        materialized="incremental",
        incremental_strategy="merge",
        incremental_predicates=[incremental_predicate("DBT_INTERNAL_DEST.block_time")],
        unique_key=["block_month", "tx_id", "token_address"],
    )
}}

{% set query_start_date = "2024-01-01" %}

with
    fee_payments as (
        select *
        from {{ ref("phantom_swapper_solana_fee_payments_raw") }}
        {% if is_incremental() %} where {{ incremental_predicate("block_time") }}
        {% else %} where block_time >= timestamp '{{query_start_date}}'
        {% endif %}
    ),
    fee_token_prices as (
        select *
        from {{ ref("phantom_swapper_solana_fee_token_prices") }}
        {% if is_incremental() %} where {{ incremental_predicate("minute") }}
        {% else %} where minute >= timestamp '{{query_start_date}}'
        {% endif %}
    )
select
    fee_payments.block_time,
    fee_payments.block_month,
    fee_payments.blockchain,
    fee_payments.amount,
    fee_token_prices.price as token_price_usd,
    fee_payments.amount * fee_token_prices.price as amount_usd,
    fee_token_prices.symbol as token_symbol,
    fee_token_prices.decimals as token_decimals,
    fee_payments.token_address,
    fee_payments.fee_receiver,
    fee_payments.tx_id,
    fee_payments.index
from fee_payments
left join
    fee_token_prices
    on (
        fee_token_prices.contract_address_base58 = fee_payments.token_address
        and fee_token_prices.minute = date_trunc('minute', fee_payments.block_time)
    )
