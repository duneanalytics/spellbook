{{
    config(
        schema = 'ton_elector'
        , alias = 'return_stake'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        , unique_key = ['tx_hash']
        , post_hook = '{{ expose_spells(\'["ton"]\',
                                    "sector",
                                    "ton",
                                    \'["pshuvalov"]\') }}'
    )
}}


with 
_messages as (
    select
        m.block_date,
        m.block_time,
        m.tx_hash,
        m.tx_lt,
        m.trace_id,
        m.value,
        m.body_boc,
        m.destination as wallet_address
    from
        {{ source('ton', 'messages') }} m
    where
        m.opcode = -294697652 -- new stake 0xee6f454c
        and m.source = '-1:3333333333333333333333333333333333333333333333333333333333333333' -- elector address
        and m.direction = 'out'
        {% if is_incremental() %}
        AND {{ incremental_predicate('m.block_date') }}
        {% endif %}
),
_parsed as (
    select try({{ ton_from_boc('body_boc', [
        ton_begin_parse(),
        ton_skip_bits(32),
        ton_load_uint(64, 'query_id'),
        ton_load_uint(32, 'reason')
        ]) }}) as result, * 
    from 
        _messages
)
select block_date, block_time, tx_hash, tx_lt, trace_id, wallet_address, value,
result.query_id, result.reason
from _parsed