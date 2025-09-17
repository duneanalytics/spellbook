{{
    config(
        schema = 'ton_elector',
        alias='new_stake',
        
        materialized = 'table',
        unique_key = ['address'],
        post_hook='{{ expose_spells(\'["ton"]\',
                                    "sector",
                                    "ton",
                                    \'["pshuvalov"]\') }}'
    )
}}


with 
_messages as (
    select
        t.block_date,
        m.block_time,
        m.tx_hash,
        m.tx_lt,
        m.trace_id,
        m.value,
        m.body_boc,
        m.source as wallet_address
    from
        {{ source('ton', 'messages') }} m
    join
        {{ source('ton', 'transactions') }} t on t.hash = m.tx_hash and t.block_date = m.block_date and m.direction = 'in'
    where
        m.opcode = from_base(substr('0x4e73744b', 3), 16) -- new stake
        and t.compute_exit_code = 0 and t.action_result_code = 0 -- check that transaction is successful
        and m.destination = '-1:3333333333333333333333333333333333333333333333333333333333333333' -- elector address
        {% if is_incremental() %}
        AND {{ incremental_predicate('m.block_date') }}
        {% endif %}
),
_parsed as (
    select try({{ ton_from_boc('body_boc', [
        ton_begin_parse(),
        ton_skip_bits(32),
        ton_load_uint(64, 'query_id'),
        ton_load_uint(256, 'validator_pubkey'),
        ton_load_uint(32, 'stake_at'),
        ton_load_uint(32, 'max_factor'),
        ton_load_uint(256, 'adnl_addr')
        ]) }}) as result, * 
    from 
        _messages
)
select block_date, block_time, tx_hash, tx_lt, trace_id, wallet_address, value,
result.query_id, result.validator_pubkey, result.stake_at, result.max_factor, result.adnl_addr
from _parsed