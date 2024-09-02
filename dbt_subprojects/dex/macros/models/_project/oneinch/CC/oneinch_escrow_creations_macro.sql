{% macro oneinch_escrow_creations_macro(blockchain) %}

{% set date_from = '2024-08-20' %}

with

factories as (
    select factory
    from ({{ oneinch_blockchain_macro(blockchain) }}), unnest(escrow_factory_addresses) as f(factory)
)

-- output --

select
    '{{ blockchain }}' as blockchain
    , block_number
    , block_time
    , tx_hash
    , trace_address
    , "from" as factory
    , address as escrow
    , code
    , success
    , tx_success
    , date_trunc('month', block_time) as block_month
from {{ source(blockchain, 'traces') }}
where
    type = 'create'
    and "from" in (select factory from factories)
    {% if is_incremental() %}
        and {{ incremental_predicate('block_time') }}
    {% else %}
        and block_time > timestamp '{{ date_from }}'
    {% endif %}

{% endmacro %}