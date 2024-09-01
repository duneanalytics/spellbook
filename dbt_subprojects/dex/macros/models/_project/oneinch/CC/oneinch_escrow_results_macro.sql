{% macro oneinch_escrow_results_macro(blockchain) %}

{% set date_from = '2024-08-20' %}
{% set selector = 'substr(input, 1, 4)' %}
{% set withdraw     = '0x23305703' %}
{% set cancel       = '0x90d3252f' %}
{% set rescueFunds  = '0x4649088b' %}



with

escrows as (
    select escrow as "to"
    from {{ source('oneinch_' + blockchain, 'escrow_creations') }}
    -- without an incremental predicate, as the results may be delayed
)

-- output --

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
    , date_trunc('month', block_time) as block_month
from {{ source(blockchain, 'traces') }}
where
    {{ selector }} in ({{ withdraw }}, {{ cancel }}, {{ rescueFunds }})
    and "to" in (select * from escrows)
    and tx_success
    and success
    and call_type = 'call'
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% else %}
        and block_time > timestamp '{{ date_from }}'
    {% endif %}

{% endmacro %}