{% set blockchain = 'arbitrum' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'ptfc_w_prices',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'block_number', 'transfer_from', 'tx_hash', 'transfer_trace_address']
    )
}}


{% set native_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}

with prices as (
    select
        contract_address,
        price,
        minute,
        decimals
    from {{ source('prices', 'usd') }}
    where blockchain = '{{ blockchain }}'
        and minute >= date('2021-05-01')
        {% if is_incremental() %}
        and {{ incremental_predicate('minute') }}
        {% endif %} 
)

, meta as (
    select 
        chain_id
        , wrapped_native_token_address
        , native_token_symbol as native_symbol
    from {{ source('oneinch', 'blockchains') }}
    where blockchain = '{{blockchain}}'
)

, transfers as (
    select 
        blockchain
        , block_number
        , block_time
        , tx_hash
        , transfer_trace_address
        , contract_address as contract_address_raw
        , if(contract_address = {{ native_address }}, wrapped_native_token_address, contract_address) as contract_address
        , contract_address = {{ native_address }} as native
        , amount
        , native_symbol
        , transfer_from
        , transfer_to
        , date_trunc('minute', block_time) as minute
        , block_month
    from (
        select *, date(date_trunc('month', block_time)) as block_month from (
        {{
            oneinch_project_ptfc_macro(
                blockchain = blockchain
            )
        }}
        )
        where block_time >= date('2021-05-01')
            {% if is_incremental() %} 
         and   {{ incremental_predicate('block_time') }}
        {% endif %}
    ), meta
)


select 
    *
    , price * amount / pow(10, decimals) as amount_usd
from transfers
left join prices using(contract_address, minute)