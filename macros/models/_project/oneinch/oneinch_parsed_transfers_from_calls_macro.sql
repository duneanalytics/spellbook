{% macro 
    oneinch_parsed_transfers_from_calls_macro(
        blockchain
    ) 
%}



{% set transfer_selector = '0xa9059cbb' %}
{% set transfer_from_selector = '0x23b872dd' %}
{% set selector = 'substr(input, 1, 4)' %}



select
    '{{ blockchain }}' as blockchain
    , block_number
    , block_time
    , tx_hash
    , trace_address as transfer_trace_address
    , if(value > uint256 '0', 0xae, "to") as contract_address
    , case {{ selector }}
        when {{ transfer_selector }} then bytearray_to_uint256(substr(input, 37, 32))
        when {{ transfer_from_selector }} then bytearray_to_uint256(substr(input, 69, 32))
        else value
    end as amount
    , case
        when {{ selector }} = {{ transfer_selector }} or value > uint256 '0' then "from"
        when {{ selector }} = {{ transfer_from_selector }} then substr(input, 17, 20)
    end as transfer_from
    , case
        when {{ selector }} = {{ transfer_selector }} then substr(input, 17, 20)
        when {{ selector }} = {{ transfer_from_selector }} then substr(input, 49, 20)
        when value > uint256 '0' then "to"
    end as transfer_to
from {{ source(blockchain, 'traces') }}
where (
        {{ selector }} = {{ transfer_selector }} and length(input) = 68
        or {{ selector }} = {{ transfer_from_selector }} and length(input) = 100
        or value > uint256 '0'
    )
    and call_type = 'call'
    and tx_success
    and success

{% endmacro %}