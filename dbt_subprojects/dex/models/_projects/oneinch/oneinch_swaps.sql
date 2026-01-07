{%- set exposed = oneinch_blockchains_cfg_macro() | selectattr("exposed") | map(attribute="name") | list -%}

{{-
    config(
        schema = 'oneinch',
        alias = 'swaps',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        partition_by = ['blockchain', 'block_month'],
        unique_key = ['blockchain', 'block_month', 'block_date', 'execution_id'],
        post_hook = '{{ expose_spells(
            blockchains = \'exposed\',
            spell_type = "project",
            spell_name = "oneinch",
            contributors = \'["max-morrow", "grkhr"]\'
        ) }}',
    )
-}}



with

executions as (
    {% for stream in oneinch_streams_cfg_macro() %}
        select *
            , {{ stream.mode }} as mode
            , false as second_side
            , {% if stream.name == 'ar' -%} remains {%- else -%} slice(remains, 1, 1) {%- endif %} as updated_remains
        from {{ ref('oneinch_' + stream.name + '_executions') }}
        where true
            {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}

    {% if stream.name == 'lo' %}
        union all -- second side of LO calls (when a direct call LO method => users from two sides)
        select *
            , 'classic' as mode
            , true as second_side
            , slice(remains, 2, cardinality(remains)) as updated_remains
        from {{ ref('oneinch_' + stream.name + '_executions') }}
        where true
            and protocol = 'LO'
            and coalesce(element_at(flags, 'direct'), false)
            {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}
        
    {% endif %}
        {% if not loop.last -%} union all {%- endif %}
    {% endfor %}
)

, resolvers as (
    select *
        , account_address as tx_from
    from {{ ref('oneinch_intent_accounts') }}
)

-- output --

select
    blockchain
    , chain_id
    , block_number
    , block_time
    , tx_hash
    , tx_success
    , tx_from
    , tx_to
    , tx_nonce
    , tx_gas_used
    , tx_gas_price
    , tx_priority_fee_per_gas
    , tx_index -- it is necessary to determine the order of creations in the block
    , call_trace_address
    , call_success
    , call_gas_used
    , call_selector
    , call_method
    , call_from
    , call_to
    , call_output
    , call_error
    , call_type
    , mode
    , protocol
    , protocol_version
    , contract_name
    , resolver_address
    , resolver_name
    , amount_usd
    , execution_cost
    , if(second_side, tx_from, user) as user
    , if(second_side, cast(null as varbinary), receiver) as receiver
    , if(second_side, dst_token_address, src_token_address) as src_token_address
    , if(second_side, dst_token_amount, src_token_amount) as src_token_amount
    , if(second_side, dst_executed_address, src_executed_address) as src_executed_address
    , if(second_side, dst_executed_symbol, src_executed_symbol) as src_executed_symbol
    , if(second_side, dst_executed_amount, src_executed_amount) as src_executed_amount
    , if(second_side, dst_executed_amount_usd, src_executed_amount_usd) as src_executed_amount_usd
    , dst_blockchain
    , if(second_side, src_token_address, dst_token_address) as dst_token_address
    , if(second_side, src_token_amount, dst_token_amount) as dst_token_amount
    , if(second_side, src_executed_address, dst_executed_address) as dst_executed_address
    , if(second_side, src_executed_symbol, dst_executed_symbol) as dst_executed_symbol
    , if(second_side, src_executed_amount, dst_executed_amount) as dst_executed_amount
    , if(second_side, src_executed_amount_usd, dst_executed_amount_usd) as dst_executed_amount_usd
    , actions
    , order_hash
    , hashlock
    , complement
    , updated_remains as remains
    , map_concat(flags, map_from_entries(array[('second_side', second_side)])) as flags
    , block_date
    , block_month
    , native_price
    , native_decimals
    , sha1(to_utf8(concat_ws('|'
        , blockchain
        , cast(tx_hash as varchar)
        , array_join(call_trace_address, ',') -- ',' is necessary to avoid similarities after concatenation // array_join(array[1, 0], '') = array_join(array[10], '')
        , mode
    ))) as execution_id
    -- additional --
    , coalesce(order_hash, sha1(to_utf8(concat_ws('|'
        , blockchain
        , cast(tx_hash as varchar)
        , array_join(call_trace_address, ',') -- ',' is necessary to avoid similarities after concatenation // array_join(array[1, 0], '') = array_join(array[10], '')
        , mode
    )))) as swap_id
    , to_unixtime(block_time) as unixtime
    , get_href(get_chain_explorer_tx_hash(blockchain, if(blockchain = 'solana', to_base58(tx_hash), cast(tx_hash as varchar))), if(blockchain = 'solana', to_base58(tx_hash), cast(tx_hash as varchar))) as tx_link
    , get_href(get_chain_explorer_address(blockchain, if(blockchain = 'solana', to_base58(user), cast(user as varchar))), if(blockchain = 'solana', to_base58(user), cast(user as varchar))) as user_link
    , get_href(get_chain_explorer_address(blockchain, if(blockchain = 'solana', to_base58(src_token_address), cast(src_token_address as varchar))), coalesce(src_executed_symbol, format('%s…%s', if(blockchain = 'solana', substr(to_base58(src_token_address), 1, 4), cast(substr(src_token_address, 1, 2) as varchar)), if(blockchain = 'solana', substr(to_base58(src_token_address), -4), substr(cast(src_token_address as varchar), -4))))) as src_link
    , get_href(get_chain_explorer_address(blockchain, if(blockchain = 'solana', to_base58(dst_token_address), cast(dst_token_address as varchar))), coalesce(dst_executed_symbol, format('%s…%s', if(blockchain = 'solana', substr(to_base58(dst_token_address), 1, 4), cast(substr(dst_token_address, 1, 2) as varchar)), if(blockchain = 'solana', substr(to_base58(dst_token_address), -4), substr(cast(dst_token_address as varchar), -4))))) as dst_link
    , coalesce(src_executed_symbol, format('%s…%s', if(blockchain = 'solana', substr(to_base58(src_token_address), 1, 4), cast(substr(src_token_address, 1, 2) as varchar)), if(blockchain = 'solana', substr(to_base58(src_token_address), -4), substr(cast(src_token_address as varchar), -4)))) as src_token
    , coalesce(dst_executed_symbol, format('%s…%s', if(blockchain = 'solana', substr(to_base58(dst_token_address), 1, 4), cast(substr(dst_token_address, 1, 2) as varchar)), if(blockchain = 'solana', substr(to_base58(dst_token_address), -4), substr(cast(dst_token_address as varchar), -4)))) as dst_token
    , (cast(dst_executed_amount as double) / pow(10, cast(element_at(complement, 'dst_decimals') as bigint))) / (cast(src_executed_amount as double) / pow(10, cast(element_at(complement, 'src_decimals') as bigint))) between 0.99 and 1.01 as stable
    , coalesce((cast(src_executed_amount as double) / pow(10, cast(element_at(complement, 'src_decimals') as bigint))) / src_executed_amount_usd, (cast(dst_executed_amount as double) / pow(10, cast(element_at(complement, 'dst_decimals') as bigint))) / dst_executed_amount_usd) between 0.99 and 1.01 as usd_stable
    , element_at(updated_remains, 1) as value
    , if(mod(length(call_output), 32) = 4, 0x) as call_output_selector
    , if(mod(length(call_output), 32) = 4, element_at(slice(regexp_extract_all(from_utf8(call_output), '[\w\d\s\.:\(\)]+'), -1, 1), 1), cast(null as varchar)) as call_output_decoded
from executions
left join resolvers using(blockchain, tx_from)