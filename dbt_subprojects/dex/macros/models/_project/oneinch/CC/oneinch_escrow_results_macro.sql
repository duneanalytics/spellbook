{% macro oneinch_escrow_results_macro(blockchain) %}

{% set date_from = '2024-08-20' %}
{% set selector = 'substr(input, 1, 4)' %}
{% set withdraw     = '0x23305703' %}
{% set cancel       = '0x90d3252f' %}
{% set rescueFunds  = '0x4649088b' %}



with

factories as (
    select factory
    from ({{ oneinch_blockchain_macro(blockchain) }}), unnest(escrow_factory_addresses) as f(factory)
)

, creations as (
    select address
    from {{ source(blockchain, 'creation_traces') }}
    where
        "from" in (select factory from factories)
        and block_time > greatest(timestamp '{{ date_from }}', timestamp {{ oneinch_easy_date() }}) -- without an incremental predicate, as the results may be delayed
)

, results as (
    select
        '{{ blockchain }}' as blockchain
        , block_number
        , block_time
        , tx_hash
        , "to" as escrow
        , trace_address
        , {{ selector}} as selector
        , case {{ selector }}
            when {{ cancel      }} then 'cancel'
            when {{ withdraw    }} then 'withdraw'
            when {{ rescueFunds }} then 'rescueFunds'
        end as method
        , case {{ selector }}
            when {{ withdraw    }} then substr(input, 4 + 32*0 + 1, 32)
            else null
        end as secret
        , substr(input, 4 + 32*case {{ selector }}
            when {{ cancel      }} then 0
            when {{ withdraw    }} then 1
            when {{ rescueFunds }} then 2
        end + 1, 32) as order_hash
        , substr(input, 4 + 32*case {{ selector }}
            when {{ cancel      }} then 1
            when {{ withdraw    }} then 2
            when {{ rescueFunds }} then 3
        end + 1, 32) as hashlock
        , substr(input, 4 + 32*case {{ selector }}
            when {{ cancel      }} then 4
            when {{ withdraw    }} then 5
            when {{ rescueFunds }} then 6
        end + 12 + 1, 20) as token
        , bytearray_to_uint256(substr(input, 4 + 32*case {{ selector }}
            when {{ cancel      }} then 5
            when {{ withdraw    }} then 6
            when {{ rescueFunds }} then 7
        end + 1, 32)) as amount
        , case {{ selector }}
            when {{ rescueFunds }} then substr(input, 4 + 32*0 + 12 + 1, 20)
            else null
        end as rescue_token
        , case {{ selector }}
            when {{ rescueFunds }} then bytearray_to_uint256(substr(input, 4 + 32*1 + 1, 32))
            else null
        end as rescue_amount
        , success as call_success
        , tx_success
    from {{ source(blockchain, 'traces') }}
    where
        {{ selector }} in ({{ withdraw }}, {{ cancel }}, {{ rescueFunds }})
        and "to" in (select address from creations)
        and call_type = 'call'
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% else %}
            and block_time > greatest(timestamp '{{ date_from }}', timestamp {{ oneinch_easy_date() }})
        {% endif %}
)

-- output --

select
    blockchain
    , block_number
    , block_time
    , tx_hash
    , trace_address
    , escrow
    , hashlock
    , selector
    , method
    , secret
    , order_hash
    , token
    , amount
    , rescue_token
    , rescue_amount
    , call_success
    , tx_success
    , date_trunc('month', block_time) as block_month
from results

{% endmacro %}