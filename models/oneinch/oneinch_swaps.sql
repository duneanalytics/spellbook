{{  
    config(
        schema = 'oneinch',
        alias = 'swaps',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        partition_by = ['block_month'],
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address', 'second_side'],
        post_hook='{{ expose_spells(\'["ethereum", "polygon", "bnb", "avalanche_c", "gnosis", "fantom", "optimism", "arbitrum", "base"]\',
                                "project",
                                "oneinch",
                                \'["grkhr", "max-morrow"]\') }}'
    )
}}



{% set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' %}
{% set true_native_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' %}



with

    settlements as (
        select
            blockchain
            , contract_address as call_from
            , true as fusion
        from {{ ref('oneinch_fusion_settlements') }}
    )

    , calls as (
        -- AR calls
        select
            blockchain
            , tx_hash
            , call_trace_address
            , contract_name
            , 'AR' as protocol
            , protocol_version
            , method
            , tx_from as user
            , dst_receiver as receiver
            , if(src_token_address in {{native_addresses}}, wrapped_native_token_address, src_token_address) as src_token_address
            , if(src_token_address in {{native_addresses}}, native_token_symbol) as src_native
            , src_amount
            , if(dst_token_address in {{native_addresses}}, wrapped_native_token_address, dst_token_address) as dst_token_address
            , if(dst_token_address in {{native_addresses}}, native_token_symbol) as dst_native
            , dst_amount
            , false as fusion
            , false as contracts_only
            , false as second_side
            , explorer_link
            , minute
        from {{ ref('oneinch_ar') }}
        join {{ ref('evms_info') }} using(blockchain)
        where
            tx_success
            and call_success
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            {% endif %}


        union all
        -- LOP calls - first side (when a direct call LOP method => users from two sides)
        select
            blockchain
            , tx_hash
            , call_trace_address
            , contract_name
            , 'LOP' as protocol
            , protocol_version
            , method
            , maker as user
            , receiver
            , if(maker_asset in {{native_addresses}}, wrapped_native_token_address, maker_asset) as src_token_address
            , if(maker_asset in {{native_addresses}}, native_token_symbol) as src_native
            , making_amount as src_amount
            , if(taker_asset in {{native_addresses}}, wrapped_native_token_address, taker_asset) as dst_token_address
            , if(taker_asset in {{native_addresses}}, native_token_symbol) as dst_native
            , taking_amount as dst_amount
            , coalesce(fusion, false) as fusion
            , position('RFQ' in method) > 0 as contracts_only
            , false as second_side
            , explorer_link
            , minute
        from {{ ref('oneinch_lop') }}
        join {{ ref('evms_info') }} using(blockchain)
        left join settlements using(blockchain, call_from)
        where
            tx_success
            and call_success
            {% if is_incremental() %}
                and {{ incremental_predicate('block_time') }}
            {% endif %}


        union all
        -- LOP calls - second side (when a direct call LOP method => users from two sides)
        select
            blockchain
            , tx_hash
            , call_trace_address
            , contract_name
            , 'LOP' as protocol
            , protocol_version
            , method
            , tx_from as user
            , receiver
            , if(taker_asset in {{native_addresses}}, wrapped_native_token_address, taker_asset) as src_token_address
            , if(taker_asset in {{native_addresses}}, native_token_symbol) as src_native
            , taking_amount as src_amount
            , if(maker_asset in {{native_addresses}}, wrapped_native_token_address, maker_asset) as dst_token_address
            , if(maker_asset in {{native_addresses}}, native_token_symbol) as dst_native
            , making_amount as dst_amount
            , false as fusion
            , false as contracts_only
            , true as second_side
            , explorer_link
            , minute
        from {{ ref('oneinch_lop') }}
        join {{ ref('evms_info') }} using(blockchain)
        where
            tx_success
            and call_success
            and cardinality(call_trace_address) = 0
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

    , swaps as (
        select
            blockchain
            , tx_hash
            , call_trace_address
            , block_time
            , minute
            , tx_from
            , tx_to
            , call_from
            , call_to
            , contract_name
            , protocol
            , protocol_version
            , method
            , user
            , receiver
            , fusion
            , contracts_only
            , second_side
            , call_remains as remains
            , any_value(explorer_link) as explorer_link
            , any_value(if(src_native is null, src_token_address, {{true_native_address}})) filter(where contract_address = src_token_address) as src_token_address
            , any_value(if(dst_native is null, dst_token_address, {{true_native_address}})) filter(where contract_address = dst_token_address) as dst_token_address
            , max(amount) filter(where contract_address = src_token_address and amount <= src_amount) as src_amount
            , max(amount) filter(where contract_address = dst_token_address and amount <= dst_amount) as dst_amount
            , any_value(coalesce(src_native, symbol)) filter(where contract_address = src_token_address) as src_token_symbol
            , any_value(coalesce(dst_native, symbol)) filter(where contract_address = dst_token_address) as dst_token_symbol
            , max(amount * price / pow(10, decimals)) filter(where contract_address = src_token_address and amount <= src_amount or contract_address = dst_token_address and amount <= dst_amount) as sources_usd_amount
            , max(amount * price / pow(10, decimals)) as transfers_usd_amount
            , count(*) as transfers
        from (
            select
                blockchain
                , tx_hash
                , call_trace_address
                , block_time
                , tx_from
                , tx_to
                , call_from
                , call_to
                , contract_address
                , minute
                , amount
                , call_remains
            from {{ ref('oneinch_calls_transfers_amounts') }}
            {% if is_incremental() %}
                where {{ incremental_predicate('block_time') }}
            {% endif %}
        )
        join calls using(blockchain, tx_hash, call_trace_address, minute)
        left join prices using(blockchain, contract_address, minute)
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
    )

select
    blockchain
    , tx_hash
    , call_trace_address
    , block_time
    , minute
    , tx_from
    , tx_to
    , call_from
    , call_to
    , contract_name
    , protocol
    , protocol_version
    , method
    , user
    , receiver
    , fusion
    , contracts_only
    , second_side
    , remains
    , src_token_address
    , dst_token_address
    , src_amount
    , dst_amount
    , src_token_symbol
    , dst_token_symbol
    , coalesce(sources_usd_amount, transfers_usd_amount) as usd_amount
    , sources_usd_amount
    , transfers_usd_amount
    , transfers
    , explorer_link
    , date(date_trunc('month', block_time)) as block_month
from swaps