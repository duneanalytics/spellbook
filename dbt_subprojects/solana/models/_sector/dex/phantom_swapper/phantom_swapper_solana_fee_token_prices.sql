{{
    config(
        schema="phantom_swapper_solana",
        alias="fee_token_prices",
        partition_by=["block_month"],
        materialized="incremental",
        incremental_strategy="merge",
        incremental_predicates=[incremental_predicate("DBT_INTERNAL_DEST.minute")],
        unique_key=["block_month", "minute", "contract_address_varbinary"],
    )
}}

{% set query_start_date = '2024-01-01' %}

with
    fee_payments as (
        select
            *,
            date_trunc('minute', block_time) as minute,
            from_base58(token_address) as contract_address_varbinary,
            token_address as contract_address_base58
        from {{ ref("phantom_swapper_solana_fee_payments_raw") }}
        {% if is_incremental() %} where {{ incremental_predicate("block_time") }}
        {% else %} where block_time >= timestamp '{{query_start_date}}'
        {% endif %}
    ),
    distinct_fee_payment_tokens_per_minute as (
        select distinct
            contract_address_varbinary, contract_address_base58, blockchain, minute
        from fee_payments
    )
select
    tokens.minute,
    cast(date_trunc('month', tokens.minute) as date) as block_month,
    tokens.blockchain,
    prices.symbol,
    prices.price,
    prices.decimals,
    tokens.contract_address_varbinary,
    tokens.contract_address_base58
from distinct_fee_payment_tokens_per_minute as tokens
join
    {{ source("prices", "usd") }} as prices
    on (
        prices.blockchain = tokens.blockchain
        and prices.contract_address = tokens.contract_address_varbinary
        and prices.minute = tokens.minute
        {% if is_incremental() %} and {{ incremental_predicate("prices.minute") }}
        {% else %} and prices.minute >= timestamp '{{query_start_date}}'
        {% endif %}
    )
