{{  
    config(
        schema = 'oneinch',
        alias = 'trades',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}

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
            -- , dst_receiver as receiver -- to add next
            , if(src_token_address in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), wrapped_native_token_address, src_token_address) as src_token_address
            , if(src_token_address in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), native_token_symbol) as src_native
            , src_amount
            , if(dst_token_address in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), wrapped_native_token_address, dst_token_address) as dst_token_address
            , if(dst_token_address in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), native_token_symbol) as dst_native
            , dst_amount
            , false as fusion
            , false as contracts_only
            , false as second_side
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
            -- , receiver -- to add next
            , if(maker_asset in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), wrapped_native_token_address, maker_asset) as src_token_address
            , if(maker_asset in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), native_token_symbol) as src_native
            , making_amount as src_amount
            , if(taker_asset in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), wrapped_native_token_address, taker_asset) as dst_token_address
            , if(taker_asset in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), native_token_symbol) as dst_native
            , taking_amount as dst_amount
            , coalesce(fusion, false) as fusion
            , position('RFQ' in method) > 0 as contracts_only
            , false as second_side
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
            -- , receiver -- to add next
            , if(taker_asset in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), wrapped_native_token_address, taker_asset) as src_token_address
            , if(taker_asset in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), native_token_symbol) as src_native
            , taking_amount as src_amount
            , if(maker_asset in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), wrapped_native_token_address, maker_asset) as dst_token_address
            , if(maker_asset in (0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee), native_token_symbol) as dst_native
            , making_amount as dst_amount
            , false as fusion
            , false as contracts_only
            , true as second_side
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

    , trades as (
        select
            blockchain
            , tx_hash
            , call_trace_address
            , min(block_time) as block_time
            , min(tx_from) as tx_from
            , min(tx_to) as tx_to
            , min(contract_name) as contract_name
            , min(protocol) as protocol
            , min(protocol_version) as protocol_version
            , min(method) as method
            , min(user) as user
            -- , min(receiver) as receiver -- to add next
            , min(fusion) as fusion
            , min(contracts_only) as contracts_only
            , min(second_side) as second_side
            , min(call_remains) as remains
            , min(src_token_address) filter(where contract_address = src_token_address) as src_token_address
            , min(dst_token_address) filter(where contract_address = dst_token_address) as dst_token_address
            , max(amount) filter(where contract_address = src_token_address and amount <= src_amount) as src_amount
            , max(amount) filter(where contract_address = dst_token_address and amount <= dst_amount) as dst_amount
            , min(coalesce(src_native, symbol)) filter(where contract_address = src_token_address) as src_token_symbol
            , min(coalesce(dst_native, symbol)) filter(where contract_address = dst_token_address) as dst_token_symbol
            , max(amount * price / pow(10, decimals)) filter(where contract_address = src_token_address and amount <= src_amount or contract_address = dst_token_address and amount <= dst_amount) as usd_amount
        from (
            select
                blockchain
                , tx_hash
                , call_trace_address
                , block_time
                , tx_from
                , tx_to
                , contract_address
                , minute
                , amount
                , transfer_from
                , transfer_to
                , call_remains
            from {{ ref('oneinch_calls_transfers_amounts') }}
        )
        join calls using(blockchain, tx_hash, call_trace_address)
        join prices using(blockchain, contract_address, minute)
        group by 1, 2, 3
    )

select *
from trades