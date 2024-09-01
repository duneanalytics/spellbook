{% macro oneinch_escrow_dst_macro(blockchain) %}

{% set date_from = '2024-08-20' %}

with

factories as (
    select factory
    from ({{ oneinch_blockchain_macro(blockchain) }}), unnest(escrow_factory_addresses) as f(factory)
)

-- will be converted to submitted contracts
, createDstEscrow as (
    select
        '{{ blockchain }}' as blockchain
        , block_number
        , block_time
        , tx_hash
        , trace_address
        , substr(input, 4 + 32*1 + 1, 32) as hashlock
        , substr(input, 4 + 32*2 + 12 + 1, 20) as maker
        , substr(input, 4 + 32*3 + 12 + 1, 20) as taker
        , substr(input, 4 + 32*4 + 12 + 1, 20) as token
        , bytearray_to_uint256(substr(input, 4 + 32*5 + 1, 32)) as amount
    from {{ source(blockchain, 'traces') }}
    where
        starts_with(input, 0xdea024e4) -- createDstEscrow
        and "to" in (select * from factories)
        {% if is_incremental() %}
            and {{ incremental_predicate('block_time') }}
        {% else %}
            and block_time > timestamp '{{ date_from }}'
        {% endif %}
)

, creations as (
    select *
    from {{ ref('oneinch_' + blockchain + '_escrow_creations') }}
    {% if is_incremental() %}where {{ incremental_predicate('block_time') }}{% endif %}
)

, results as (
    select
        escrow
        , hashlock
        , token
        , sum(amount) filter(where method = 'cancel') as cancel_amount
        , sum(amount) filter(where method = 'withdraw') as withdraw_amount
        , sum(amount) filter(where method = 'rescueFunds') as rescue_amount
        , max_by(block_time, amount) filter(where method = 'cancel') as main_cancel_time
        , max_by(block_time, amount) filter(where method = 'withdraw') as main_withdraw_time
        , max_by(block_time, amount) filter(where method = 'rescueFunds') as main_rescue_time
        , array_agg(distinct tx_hash) filter(where method = 'cancel') as cancels
        , array_agg(distinct tx_hash) filter(where method = 'withdraw') as withdrawals
        , array_agg(distinct tx_hash) filter(where method = 'rescueFunds') as rescues
    from {{ ref('oneinch_' + blockchain + '_escrow_results') }}
    -- with an incremental predicate, as the results always come after the creations
    {% if is_incremental() %}where {{ incremental_predicate('block_time') }}{% endif %}
    group by 1, 2, 3
)

-- output --

select
    createDstEscrow.blockchain
    , createDstEscrow.block_number
    , createDstEscrow.block_time
    , createDstEscrow.tx_hash
    , createDstEscrow.trace_address
    , createDstEscrow.hashlock
    , createDstEscrow.maker
    , createDstEscrow.taker
    , createDstEscrow.token
    , createDstEscrow.amount
    , creations.factory
    , creations.escrow
    , results.withdraw_amount
    , results.cancel_amount
    , results.rescue_amount
    , results.main_withdraw_time
    , results.main_cancel_time
    , results.main_rescue_time
    , results.withdrawals
    , results.cancels
    , results.rescues
    , date_trunc('month', createDstEscrow.block_time) as block_month
from createDstEscrow
left join creations on
    creations.block_number = createDstEscrow.block_number
    and creations.tx_hash = createDstEscrow.tx_hash
    and slice(creations.trace_address, 1, cardinality(createDstEscrow.trace_address)) = createDstEscrow.trace_address
left join results on
    results.escrow = creations.escrow
    and results.token = createDstEscrow.token

{% endmacro %}