{{  
    config(
        schema = 'oneinch',
        alias = 'swaps',
        materialized = 'view',
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address', 'second_side']
    )
}}



{% set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' %}



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

        union all 

        -- performance optimization
        select 
            blockchain
            , contract_address
            , minute
            , null as price
            , null as decimals
            , null as symbol 
        from {{ ref('oneinch_calls_transfers_amounts') }} as cta
        where not exists (
            select blockchain, contract_address, minute from {{ source('prices', 'usd') }} as pu
            where cta.blockchain = pu.blockchain and cta.contract_address = pu.contract_address and cta.minute = pu.minute
        )
    )

    , trades as (
        select
            blockchain
            , tx_hash
            , call_trace_address
            , block_time
            , minute
            , protocol
            , tx_from
            , tx_to
            , contract_name
            , protocol_version
            , method
            , user
            , receiver
            , fusion
            , contracts_only
            , second_side
            , call_remains as remains
            , any_value(explorer_link) as explorer_link
            , any_value(src_token_address) filter(where contract_address = src_token_address) as src_token_address
            , any_value(dst_token_address) filter(where contract_address = dst_token_address) as dst_token_address
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
                , contract_address
                , minute
                , amount
                , transfer_from
                , transfer_to
                , call_remains
            from {{ ref('oneinch_calls_transfers_amounts') }}
        )
        join calls using(blockchain, tx_hash, call_trace_address, minute)
        join prices using(blockchain, contract_address, minute)
        group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
    )

select
    blockchain
    , tx_hash
    , call_trace_address
    , block_time
    , minute
    , tx_from
    , tx_to
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
from trades
