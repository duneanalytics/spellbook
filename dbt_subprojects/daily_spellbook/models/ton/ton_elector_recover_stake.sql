{{
    config(
        schema = 'ton_elector'
        , alias = 'recover_stake'
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
        m.value,
        m.destination as wallet_address
    from
        {{ source('ton', 'messages') }} m
    where
        m.opcode = -110136540 -- recover stake
        and m.direction = 'out'
        and m.source = '-1:3333333333333333333333333333333333333333333333333333333333333333' -- elector address
        {% if is_incremental() %}
        AND {{ incremental_predicate('m.block_date') }}
        {% endif %}
)
select * from _messages