{{  
    config(
        schema = 'oneinch',
        alias = 'ar_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_month'],
        unique_key = ['blockchain', 'tx_hash', 'trace_address']
    )
}}



{% set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' %}
{% set true_native_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}



with

    calls as (
        -- AR calls
        select
            blockchain
            , tx_hash
            , call_trace_address
            , protocol_version
            , tx_from as user
            , if(src_token_address in {{native_addresses}}, wrapped_native_token_address, src_token_address) as src_token_address
            , if(src_token_address in {{native_addresses}}, native_token_symbol) as src_native
            , src_amount
            , if(dst_token_address in {{native_addresses}}, wrapped_native_token_address, dst_token_address) as dst_token_address
            , if(dst_token_address in {{native_addresses}}, native_token_symbol) as dst_native
            , dst_amount
            , minute
        from {{ ref('oneinch_ar') }}
        join {{ ref('evms_info') }} using(blockchain)
        where
            tx_success
            and call_success
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            {% endif %}
    )

    , prices as (
        select
            blockchain
            , contract_address
            , minute
            , price
            , decimals
            , symbol
        from {{ source('prices', 'usd') }}
        {% if is_incremental() %}
            where {{ incremental_predicate('minute') }}
        {% endif %}
    )

    , trades as (
        select
            blockchain
            , tx_hash
            , call_trace_address
            , block_time
            , minute
            , tx_from
            , tx_to
            , protocol_version
            , call_to
            , user
            , any_value(if(src_native is null, src_token_address, {{true_native_address}})) filter(where contract_address = src_token_address) as src_token_address
            , any_value(if(dst_native is null, dst_token_address, {{true_native_address}})) filter(where contract_address = dst_token_address) as dst_token_address
            , max(amount) filter(where contract_address = src_token_address and amount <= src_amount) as src_amount
            , max(amount) filter(where contract_address = dst_token_address and amount <= dst_amount) as dst_amount
            , max(cast(amount as double) / pow(10, decimals)) filter(where contract_address = src_token_address and amount <= src_amount) as src_amount_decimals
            , max(cast(amount as double) / pow(10, decimals)) filter(where contract_address = dst_token_address and amount <= dst_amount) as dst_amount_decimals
            , any_value(coalesce(src_native, symbol)) filter(where contract_address = src_token_address) as src_token_symbol
            , any_value(coalesce(dst_native, symbol)) filter(where contract_address = dst_token_address) as dst_token_symbol
            , max(amount * price / pow(10, decimals)) filter(where contract_address = src_token_address and amount <= src_amount or contract_address = dst_token_address and amount <= dst_amount) as sources_usd_amount
            , max(amount * price / pow(10, decimals)) as transfers_usd_amount
        from (
            select
                blockchain
                , tx_hash
                , call_trace_address
                , block_time
                , tx_from
                , tx_to
                , contract_address
                , call_from
                , call_to
                , minute
                , amount
            from {{ ref('oneinch_calls_transfers_amounts') }}
            {% if is_incremental() %}
                where {{ incremental_predicate('block_time') }}
            {% endif %}
        )
        join calls using(blockchain, tx_hash, call_trace_address, minute)
        left join prices using(blockchain, contract_address, minute)
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    )

select
    blockchain
    , '1inch' as project
    , 'AR v' || protocol_version as version
    , date_trunc('day', block_time) as block_date
    , date(date_trunc('month', block_time)) as block_month
    , block_time
    , coalesce(dst_token_symbol, '') as token_bought_symbol
    , coalesce(src_token_symbol, '') as token_sold_symbol
    , array_join(array_sort(array[coalesce(src_token_symbol, ''), coalesce(dst_token_symbol, '')]), '-') as token_pair
    , dst_amount_decimals as token_bought_amount
    , src_amount_decimals as token_sold_amount
    , dst_amount as token_bought_amount_raw
    , src_amount as token_sold_amount_raw
    , coalesce(sources_usd_amount, transfers_usd_amount) as amount_usd
    , dst_token_address as token_bought_address
    , src_token_address as token_sold_address
    , user as taker
    , cast(null as varbinary) as maker
    , call_to as project_contract_address
    , tx_hash
    , tx_from
    , tx_to
    , call_trace_address as trace_address
    , -1 as evt_index
from trades