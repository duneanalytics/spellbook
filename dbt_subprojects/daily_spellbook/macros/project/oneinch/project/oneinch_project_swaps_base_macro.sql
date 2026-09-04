{%- macro
    oneinch_project_swaps_base_macro(
        blockchain,
        date_from = '2019-01-01',
        date_to = '2049-01-01',
        easy_dates = false
    )
-%}

{%- set native_addresses = '(0x0000000000000000000000000000000000000000, 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee)' -%}
{%- set native_address = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' -%}
{%- set zero_address = '0x0000000000000000000000000000000000000000' -%}



with

meta as (
    select
        chain_id
        , wrapped_native_token_address
        , native_token_symbol as native_symbol
    from {{ source('oneinch', 'blockchains') }}
    where blockchain = '{{ blockchain }}'
)

, orders as (
    select
        block_month
        , block_number
        , tx_hash
        , call_trace_address
        , project as order_project
        , order_hash
        , maker
        , taker
        , maker_asset
        , making_amount
        , taker_asset
        , taking_amount
        , flags as order_flags
    from {{ ref('oneinch_' + blockchain + '_project_orders') }}
    where true
        and call_success
        and block_time >= timestamp '{{ date_from }}'
        and block_time < {% if easy_dates -%} date('{{ date_from }}') + interval '2' day {%- else -%} date('{{ date_to }}') {%- endif %}
        {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}
    
    union all
    
    select
        block_month
        , block_number
        , tx_hash
        , call_trace_address
        , '1inch' as order_project
        , coalesce(order_hash, concat(tx_hash, to_utf8(array_join(call_trace_address, ',')))) as order_hash
        , maker
        , receiver as taker
        , maker_asset
        , making_amount
        , taker_asset
        , taking_amount
        , flags as order_flags
    from {{ source('oneinch_' + blockchain, 'lo') }}
    where true
        and call_success
        and block_time >= timestamp '{{ date_from }}'
        and block_time < {% if easy_dates -%} date('{{ date_from }}') + interval '2' day {%- else -%} date('{{ date_to }}') {%- endif %}
        {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}
)

, calls as (
    select *
        , array_agg(call_trace_address) over(partition by block_month, block_number, tx_hash, project) as call_trace_addresses
    from {{ ref('oneinch_' + blockchain + '_project_calls') }}
    where true
        and call_success
        and (tx_success or tx_success is null)
        and (flags['cross_chain'] or not flags['cross_chain_method']) -- without cross-chain methods calls in non cross-chain protocols
        and block_time >= timestamp '{{ date_from }}'
        and block_time < {% if easy_dates -%} date('{{ date_from }}') + interval '2' day {%- else -%} date('{{ date_to }}') {%- endif %}
        {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}
)

, swaps as (
    select
        blockchain
        , block_month
        , block_number
        , tx_hash
        , tx_from
        , tx_to
        , call_trace_address
        , coalesce(order_project, project) as project
        , tag
        , flags
        , call_selector
        , method
        , call_from
        , call_to
        , order_hash
        , order_flags
        , maker
        , taker
        , replace(maker_asset, {{ zero_address }}, {{ native_address }}) as maker_asset
        , making_amount
        , replace(taker_asset, {{ zero_address }}, {{ native_address }}) as taker_asset
        , taking_amount
        , array_agg(call_trace_address) over(partition by block_month, block_number, tx_hash, coalesce(order_project, project)) as call_trace_addresses -- to update the array after filtering nested calls of the project
        , if(maker_asset in {{native_addresses}}, wrapped_native_token_address, maker_asset) as _maker_asset
        , if(taker_asset in {{native_addresses}}, wrapped_native_token_address, taker_asset) as _taker_asset
        , coalesce(order_hash, concat(tx_hash, to_utf8(array_join(call_trace_address, ',')))) as call_trade_id -- without call_trade for the correctness of the max transfer approach
    from calls
    left join orders using(block_month, block_number, tx_hash, call_trace_address), meta
    where
        reduce(call_trace_addresses, true, (r, x) -> if(r and x <> call_trace_address and slice(call_trace_address, 1, cardinality(x)) = x, false, r), r -> r) -- only not nested calls of the project in tx
        or order_hash is not null -- all orders
)

, trusted_tokens as (
    select
        distinct contract_address
        , true as trusted
    from {{ source('prices', 'trusted_tokens') }}
    where blockchain = '{{ blockchain }}'
)

, prices as (
    select
        contract_address
        , minute
        , price
        , decimals
    from {{ source('prices', 'usd') }}
    where true
        and blockchain = '{{ blockchain }}'
        and minute >= timestamp '{{ date_from }}'
        and minute < {% if easy_dates -%} date('{{ date_from }}') + interval '2' day {%- else -%} date('{{ date_to }}') {%- endif %}
        {% if is_incremental() -%} and {{ incremental_predicate('minute') }} {%- endif %}
)

, creations as (
    select address, max(block_number) as block_number
    from (
        select address, block_number
        from {{ source(blockchain, 'creation_traces') }}
        
        union all
        
        select wrapped_native_token_address, 0
        from meta
        
        union all
        
        values (0x0000000000000000000000000000000000000000, 0)
    )
    group by 1
)

, transfers as (
    select
        block_number
        , block_time
        , tx_hash
        , trace_address as transfer_trace_address
        , contract_address as contract_address_raw -- original
        , if(token_standard = 'native', wrapped_native_token_address, contract_address) as contract_address
        , token_standard = 'native' as native
        , symbol
        , amount_raw as amount
        , native_symbol
        , "from" as transfer_from
        , "to" as transfer_to
        , block_month
        , block_date
        , date_trunc('minute', block_time) as minute
    from {{ source('tokens_' + blockchain, 'transfers_from_traces') }}, meta
    where true
        and block_time >= timestamp '{{ date_from }}'
        and block_time < {% if easy_dates -%} date('{{ date_from }}') + interval '2' day {%- else -%} date('{{ date_to }}') {%- endif %}
        {% if is_incremental() -%} and {{ incremental_predicate('block_time') }} {%- endif %}
)

, swaps_unique as (
    -- One row per merge key, which `swaps` does not guarantee: measured on ethereum
    -- 2026-08-01..08, `swaps` yields 794943 rows for 794941 distinct
    -- (block_month, block_number, tx_hash, call_trace_address, call_trade_id).
    --
    -- Two rows in 795k, and they are fatal here: the model merges on those keys and Trino
    -- refuses the statement outright --
    --   "One MERGE target table row matched more than one source row"
    --
    -- The pre-change code never saw this. Its `joined` CTE aggregated the swaps-to-transfers
    -- join with group by and any_value(), which collapsed such pairs on the way through.
    -- Selecting from `swaps` without aggregating lets them survive -- and `call_transfers`
    -- reads `swaps` too, so a duplicated swap would double every transfer nested under it
    -- and double that call's net flow, which is a wrong number rather than a failed run.
    --
    -- Which of the pair survives is arbitrary, exactly as any_value() made it arbitrary
    -- before.
    select * from (
        select *, row_number() over (
            partition by block_month, block_number, tx_hash, call_trace_address, call_trade_id
            order by call_selector
        ) as _swap_rn
        from swaps
    ) where _swap_rn = 1
)
, call_transfers as (
    -- Every transfer nested under a swap call, before any sizing decision -- shared by
    -- the token/sender/receiver labels below (unchanged) and the net-flow sizing that
    -- replaces the old max-single-transfer approach (see net_legs).
    select
        swaps.block_month
        , swaps.block_number
        , swaps.tx_hash
        , swaps.call_trace_address
        , swaps.call_trade_id
        , swaps.call_from
        , swaps.call_to
        , contract_address
        , native
        , symbol
        , native_symbol
        , contract_address_raw
        , transfer_from
        , transfer_to
        , amount
        , block_time
        , minute
        , transfer_trace_address
    from swaps_unique swaps
    join transfers on true
        and swaps.block_month = transfers.block_month
        and swaps.block_number = transfers.block_number
        and swaps.tx_hash = transfers.tx_hash
        and slice(transfer_trace_address, 1, cardinality(call_trace_address)) = call_trace_address -- nested transfers only
        and reduce(array_distinct(call_trace_addresses), call_trace_address, (r, x) -> if(slice(transfer_trace_address, 1, cardinality(x)) = x and x > r, x, r), r -> r) = call_trace_address -- transfers related to the call only
        and (order_hash is null or contract_address in (_maker_asset, _taker_asset) and cardinality(array_intersect(array[call_from, maker, taker], array[transfer_from, transfer_to])) > 0) -- transfers related to the order only
)

, labels as (
    -- Token/sender/receiver labels, unchanged from before this fix -- these are display
    -- lists, not sizing, so they stay keyed off individual transfers.
    -- Keys qualified with call_transfers: this CTE joins `creations` twice and `creations`
    -- has a block_number of its own, so an unqualified block_number is ambiguous and Trino
    -- rejects the statement:
    --   Column 'block_number' is ambiguous
    -- The pre-change code did not hit this because the CTE these lines came from selected
    -- swaps.block_number explicitly.
    select
        call_transfers.block_month
        , call_transfers.block_number
        , call_transfers.tx_hash
        , call_transfers.call_trace_address
        , call_transfers.call_trade_id
        , any_value(block_time) as block_time
        , array_agg(distinct
            cast(row(if(native, native_symbol, symbol), contract_address_raw)
                as row(symbol varchar, contract_address_raw varbinary))
        ) as tokens
        , array_agg(distinct
            cast(row(if(native, native_symbol, symbol), contract_address_raw)
                as row(symbol varchar, contract_address_raw varbinary))
        ) filter(where creations_from.block_number is null or creations_to.block_number is null) as user_tokens
        , array_agg(distinct
            cast(row(if(native, native_symbol, symbol), contract_address_raw)
                as row(symbol varchar, contract_address_raw varbinary))
        ) filter(where transfer_from = call_from or transfer_to = call_from) as caller_tokens
        , array_agg(distinct transfer_from) filter(where creations_from.block_number is null) as senders
        , array_agg(distinct transfer_to) filter(where creations_to.block_number is null) as receivers
    from call_transfers
    left join creations as creations_from on creations_from.address = call_transfers.transfer_from
    left join creations as creations_to on creations_to.address = call_transfers.transfer_to
    group by 1, 2, 3, 4, 5
)

, net_transfers as (
    -- Collapse the native/wrapped pair before netting.
    --
    -- transfers_from_traces emits a wrap as TWO rows sharing one trace_address: the native
    -- leg and a wrapped-token leg, same amount, same direction. The `transfers` CTE above
    -- then maps native onto wrapped_native_token_address so it can be priced, so both rows
    -- arrive here under the same contract_address and ADD instead of cancelling.
    --
    -- Under the old max() this was invisible: the duplicate equalled the real leg, so the
    -- maximum did not move. Under net flow it doubles that leg. Measured on ethereum
    -- 2026-08-19: 67564 such pairs in the day, 100% identical in amount and 100% in
    -- direction; of 30029 swaps whose size grew, 20581 grew by exactly 2x, carrying $62.4m
    -- of the $82.7m the day gained. Day totals against production's max() figures:
    --
    --     max (production)     $1,176,198,549.89
    --     net, with the pair   $1,258,527,698.07   +7.000%
    --     net, collapsed       $1,195,401,680.58   +1.63%
    --
    -- Those three are ONE DAY of ethereum alone, which is where the wrapped-native pair was
    -- found and is not a good sample of the change's size: ethereum carries more wrapping
    -- and more flash-loan activity than the average chain. On a whole month across all 13
    -- chains, old and new computed against the same data:
    --
    --     2025-06, 13 chains
    --       max()      266,591,088 rows   $327,588,940,515.19
    --       net flow   266,591,088 rows   $328,303,462,331.25   +0.218%
    --
    -- +0.218%, not +1.63%, is the figure to review this change on. The row count is
    -- identical between them: this re-sizes swaps, it does not add or drop any.
    --
    -- A -0.978% stood here before and was wrong in sign as well as magnitude. It came from
    -- the collapse dropping transfers shared between calls, fixed two commits ago.
    --
    -- so collapsing the pair is what makes this change move volume DOWN, as intended,
    -- rather than up. Control: one Tokenlon swap reads $1,143,565.00 under max,
    -- $2,287,130.00 with the pair, and $1,143,565.00 again once collapsed, to the cent.
    --
    -- Netting path only. `labels` keeps both rows: dropping one there would drop a symbol
    -- from the token lists, and those are display lists, not sizing.
    select block_month, block_number, tx_hash, call_trace_address, call_trade_id
         , call_from, call_to, contract_address, transfer_from, transfer_to, amount, minute
    from (
        select *, row_number() over (
            -- call_trace_address and call_trade_id belong in this key. A transfer can be
            -- nested under more than one call of the same transaction -- 58665 of 728590
            -- rows on ethereum 2026-08-19 -- so partitioning without the call collapses
            -- copies belonging to DIFFERENT calls and keeps an arbitrary one. That is a
            -- transfer silently missing from every other call it belongs to, and which one
            -- survives changes between runs.
            partition by tx_hash, transfer_trace_address, contract_address,
                         transfer_from, transfer_to, amount,
                         call_trace_address, call_trade_id
            order by native
        ) as _rn
        from call_transfers
    )
    where _rn = 1
)

, net_legs as (
    -- signed_amount is a double. Both integer routes were tried against production and both
    -- overflow, on different data:
    --
    --   -amount            "Overflow in INT256 cast of UINT256: 1000000...0000" (78 digits)
    --                      -- negation casts to int256, one bit smaller in magnitude, so a
    --                      single junk amount around 1e77 is enough.
    --   sum() per side     "UINT256 addition overflow: 1045470495735148268066579153640..."
    --                      -- avoids the cast, but two such amounts in one group overflow
    --                      uint256 itself. Three attempts on 2026-03, all the same.
    --
    -- Avoiding the cast moves the ceiling, it does not remove one. Both kinds of amount are
    -- real: scam tokens, several with homoglyph symbols, none of them priced.
    --
    -- Doubles have neither ceiling and cost nothing here. Measured against the per-side
    -- integer form on ethereum 2026-08-19, in one query on one snapshot: all 144362 swaps
    -- agree to the cent, zero rows differ, zero appear in one and not the other.
    --
    -- Cancellation, which is what netting is for, still holds exactly -- two transfers of an
    -- identical raw amount give identical doubles and sum to exactly zero.
    select block_month, block_number, tx_hash, call_trace_address, call_trade_id, call_from, call_to
        , contract_address, transfer_from as address, -cast(amount as double) as signed_amount, minute
    from net_transfers
    union all
    select block_month, block_number, tx_hash, call_trace_address, call_trade_id, call_from, call_to
        , contract_address, transfer_to as address, cast(amount as double) as signed_amount, minute
    from net_transfers
)

, net_flows as (
    select
        block_month, block_number, tx_hash, call_trace_address, call_trade_id
        , any_value(call_from) as call_from
        , any_value(call_to) as call_to
        , contract_address
        , address
        , sum(signed_amount) as net_amount
        , max(minute) as minute -- one call, one tx -- transfers of the same token share a minute in practice
    from net_legs
    group by block_month, block_number, tx_hash, call_trace_address, call_trade_id, contract_address, address
)

, net_amounts as (
    select
        net_flows.block_month
        , net_flows.block_number
        , net_flows.tx_hash
        , net_flows.call_trace_address
        , net_flows.call_trade_id
        , max(abs(net_amount) * price / pow(10, decimals)) as call_amount_usd
        , max(abs(net_amount) * price / pow(10, decimals)) filter(where trusted) as call_amount_usd_trusted
        , max(abs(net_amount) * price / pow(10, decimals)) filter(where creations.block_number is null) as user_amount_usd
        , max(abs(net_amount) * price / pow(10, decimals)) filter(where creations.block_number is null and trusted) as user_amount_usd_trusted
        , max(abs(net_amount) * price / pow(10, decimals)) filter(where net_flows.address = call_from) as caller_amount_usd
        , max(abs(net_amount) * price / pow(10, decimals)) filter(where net_flows.address = call_from and trusted) as caller_amount_usd_trusted
        , max(abs(net_amount) * price / pow(10, decimals)) filter(where net_flows.address = call_to) as contract_amount_usd
        , max(abs(net_amount) * price / pow(10, decimals)) filter(where net_flows.address = call_to and trusted) as contract_amount_usd_trusted
    from net_flows
    left join prices using(contract_address, minute)
    left join trusted_tokens using(contract_address)
    left join creations on creations.address = net_flows.address
    group by net_flows.block_month, net_flows.block_number, net_flows.tx_hash, net_flows.call_trace_address, net_flows.call_trade_id
)

, joined as (
    -- Keys NOT qualified with swaps., although the CTE this replaced did qualify them:
    -- both joins below are JOIN ... USING, which merges the joined column into a single
    -- unqualified one, and swaps.block_month then does not resolve at all:
    --   Column 'swaps.block_month' cannot be resolved
    select
        blockchain
        , block_month
        , block_number
        , tx_hash
        , call_trace_address
        , call_trade_id
        , block_time
        , tx_from
        , tx_to
        , project
        , tag
        , flags
        , call_from
        , call_to
        , call_selector
        , method
        , order_hash
        , maker
        , maker_asset
        , making_amount
        , taker_asset
        , taking_amount
        , order_flags
        , tokens
        , user_tokens
        , caller_tokens
        , senders
        , receivers
        , call_amount_usd
        , call_amount_usd_trusted
        , user_amount_usd
        , user_amount_usd_trusted
        , caller_amount_usd
        , caller_amount_usd_trusted
        , contract_amount_usd
        , contract_amount_usd_trusted
    from swaps_unique swaps
    join labels using(block_month, block_number, tx_hash, call_trace_address, call_trade_id)
    left join net_amounts using(block_month, block_number, tx_hash, call_trace_address, call_trade_id)
)

, processing as (
    select *
        , array_union(senders, receivers) as users
        , array_agg(
            cast(row(
                project
                , call_trace_address
                , tokens
                , if(
                    user_amount_usd is null or caller_amount_usd is null
                    , coalesce(user_amount_usd, caller_amount_usd, call_amount_usd)
                    , greatest(user_amount_usd, caller_amount_usd)
                )
                , order_hash is not null
            ) as row(
                project varchar
                , call_trace_address array(bigint)
                , tokens array(row(symbol varchar, contract_address_raw varbinary))
                , amount_usd double
                , intent boolean
            ))
        ) over(partition by block_month, block_number, tx_hash) as tx_swaps
        , if(
            user_amount_usd_trusted is not null or caller_amount_usd_trusted is not null or contract_amount_usd_trusted is not null
            , greatest(coalesce(user_amount_usd_trusted, 0), coalesce(caller_amount_usd_trusted, 0), coalesce(contract_amount_usd_trusted, 0))
            , if(
                user_amount_usd is not null or caller_amount_usd is not null or contract_amount_usd is not null
                , greatest(coalesce(user_amount_usd, 0), coalesce(caller_amount_usd, 0), coalesce(contract_amount_usd, 0))
                , coalesce(call_amount_usd_trusted, call_amount_usd)
            )
        ) as amount_usd
        , coalesce(element_at(order_flags, 'fusion'), false) or coalesce(element_at(order_flags, 'auction'), false) as auction -- 1inch Fusion or any other auction
        , coalesce(element_at(order_flags, 'cross_chain'), false) -- 1inch cross-chain
            or coalesce(element_at(flags, 'cross_chain'), false) and not coalesce(element_at(flags, 'multi'), false) -- any suitable swap method call of exclusively cross-chain protocol
            or coalesce(element_at(flags, 'cross_chain'), false) and coalesce(element_at(flags, 'cross_chain_method'), false) -- calls of exclusively cross-chain methods of any cross-chain protocol
        as cross_chain
        , not flags['user']
            or position('RFQ' in method) > 0
            or coalesce(element_at(order_flags, 'partial') and not element_at(order_flags, 'multiple'), false)
        as contracts_only
    from joined
)

, sides as (
    select *
        , map_from_entries(array[
              ('intra-chain: classic: direct',
                entry
                and order_hash is null
                and not auction
                and not cross_chain
                and not contracts_only
            )
            , ('intra-chain: classic: external',
                not entry
                and order_hash is null
                and not auction
                and not contracts_only
                and not cross_chain
            )
            , ('intra-chain: intents: auction',
                auction
                and not cross_chain
            )
            , ('intra-chain: intents: user limit order',
                order_hash is not null
                and not auction
                and not cross_chain
                and not contracts_only
            )
            , ('intra-chain: intents: contracts only', contracts_only)
            , ('cross-chain', cross_chain)
        ]) as modes
    from (
        select *
            , flags['direct'] or reduce(tx_swaps, false, (r, x) -> x.call_trace_address <> call_trace_address and slice(call_trace_address, 1, cardinality(x.call_trace_address)) = x.call_trace_address or r, r -> not r) as entry
        from processing
    )
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , tx_from
    , tx_to
    , call_trace_address
    , project
    , tag
    , map_concat(flags, map_from_entries(array[
          ('auction', auction and not cross_chain)
        , ('cross_chain', cross_chain)
    ])) as flags
    , call_selector
    , method
    , call_from
    , call_to
    , coalesce(maker, tx_from) as user
    , order_hash
    , maker
    , maker_asset
    , making_amount
    , taker_asset
    , taking_amount
    , order_flags
    , tokens
    , user_tokens
    , caller_tokens
    , amount_usd
    , user_amount_usd
    , user_amount_usd_trusted
    , caller_amount_usd
    , caller_amount_usd_trusted
    , contract_amount_usd
    , contract_amount_usd_trusted
    , call_amount_usd
    , call_amount_usd_trusted
    , tx_swaps
    , if(cardinality(users) = 0 or order_hash is null, array_union(users, array[tx_from]), users) as users
    , users as direct_users
    , senders
    , receivers
    , date(block_time) as block_date
    , block_month
    , call_trade_id
    , order_hash is not null as intent -- and not second_side
    , entry
    , false as second_side
    , contracts_only
    , modes
    , reduce(map_keys(modes), 0, (r, x) -> r + if(modes[x], 1, 0), r -> r) as modes_count
    , reduce(map_keys(modes), 'other', (r, x) -> if(r = 'other' and modes[x], x, r), r -> r) as mode
    , sha256(to_utf8(concat_ws('|'
        , blockchain
        , cast(tx_hash as varchar)
        , cast(false as varchar) -- second_side
        , array_join(call_trace_address, ',') -- ',' is necessary to avoid similarities after concatenation // array_join(array[1, 0], '') = array_join(array[10], '')
        , cast(call_trade_id as varchar)
    ))) as id
from sides

{%- endmacro -%}