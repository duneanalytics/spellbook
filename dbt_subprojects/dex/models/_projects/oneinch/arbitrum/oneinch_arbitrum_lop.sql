{% set blockchain = 'arbitrum' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'lop',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'tx_hash', 'call_trace_address']
    )
}}


with

creations as (
    select
        block_number as creation_block_number
        , tx_hash as creation_tx_hash
        , trace_address
        , factory
        , escrow
    from {{ source('oneinch_' + blockchain, 'escrow_creations') }}
    {% if is_incremental() %}where {{ incremental_predicate('block_time') }}{% endif %}
)

, results as (
    select
        hashlock
        , escrow
        , token
        , sum(amount) filter(where selector = {{ withdraw    }}) as withdraw_amount
        , sum(amount) filter(where selector = {{ cancel      }}) as cancel_amount
        , sum(amount) filter(where selector = {{ rescueFunds }}) as rescue_amount
        , max_by(block_time, amount) filter(where selector = {{ withdraw    }}) as main_withdraw_time
        , max_by(block_time, amount) filter(where selector = {{ cancel      }}) as main_cancel_time
        , max_by(block_time, amount) filter(where selector = {{ rescueFunds }}) as main_rescue_time
        , array_agg(distinct tx_hash) filter(where selector = {{ withdraw    }}) as withdrawals
        , array_agg(distinct tx_hash) filter(where selector = {{ cancel      }}) as cancels
        , array_agg(distinct tx_hash) filter(where selector = {{ rescueFunds }}) as rescues
    from {{ source('oneinch_' + blockchain, 'escrow_results') }}
    {% if is_incremental() %}where {{ incremental_predicate('block_time') }}{% endif %}
    -- with an incremental predicate, as the results always come after the creations
    group by 1, 2, 3
)

select
    orders.*
    , factory as escrow_factory
    , hashlock
    , creations.escrow
    , withdraw_amount
    , cancel_amount
    , rescue_amount
    , main_withdraw_time
    , main_cancel_time
    , main_rescue_time
    , withdrawals
    , cancels
    , rescues
from ({{ oneinch_lop_macro(blockchain) }}) as orders
left join creations on
    creations.creation_block_number = orders.block_number
    and creations.creation_tx_hash = orders.tx_hash
    and slice(creations.trace_address, 1, cardinality(orders.call_trace_address)) = orders.call_trace_address
left join results on
    results.escrow = creations.escrow
    and results.token = orders.maker_asset