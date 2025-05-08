{{
    config(

        schema = 'bgw_solana',
        alias = 'swap',
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'tx_id'],
        post_hook='{{ expose_spells(\'["solana"]\',
                                "project",
                                "bgw",
                                \'["clyde.ren"]\') }}'
    )
}}

with swap as (
    select id, owner ref
    from {{ source('solana', 'transactions') }} a
    cross join unnest (pre_token_balances) as t1 (address, token, owner, balance_before)
    where owner in ('69xpHRvBFzG5UooTszUEEcNvdEvKZeUdbxLE2wXUbLar','AkTgH1uW6J6j6QHmFNGzZuZwwXaHQsPCpHUriED28tRj') 
    {% if is_incremental() %}
    AND {{ incremental_predicate('a.block_time') }}
    {% endif %}
    group by id, owner
)
,jup as (
    select tx_id, a.account ref
    from (
        select tx_id, account_arguments from {{ source('solana', 'instruction_calls') }} a
        where executing_account = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA'
        {% if is_incremental() %}
        AND {{ incremental_predicate('a.block_time') }}
        {% endif %}
        ) a 
    cross join (
        select account_arguments[4] account
        from {{ source('solana', 'instruction_calls') }} a
        where executing_account = 'REFER4ZgmyYx9c6He5XfaTMiGfdLwRnkV4RPp9t9iF3'
        and contains(account_arguments, '5u7y9do39ez9TRX1PwTyiTGPCdKBw2HCXgpnRVdic1EU') = true
        and cardinality(account_arguments) > 4
        {% if is_incremental() %}
        AND {{ incremental_predicate('a.block_time') }}
        {% endif %}
        ) as b
    where contains(account_arguments, account) = true
    group by tx_id, a.account
)
,bridge as (
    select
        tx_id, executing_account ref
    from
        {{ source('solana', 'instruction_calls') }} a
    where
        executing_account = '3jGqysJ7RseXMvi7finPmgEBFnvgafVjVAZ1eUBXjA7b'
        {% if is_incremental() %}
        AND {{ incremental_predicate('a.block_time') }}
        {% endif %}
    GROUP BY
        tx_id, executing_account
)
select 
    a.tx_id, trade_source, blockchain, block_time, date(block_time) block_date
    ,trader_id, amount_usd, coalesce(a.trade_souce,swap.ref,jup.ref,bridge.ref) ref
from {{ source('dex_solana', 'trades') }} a 
left join swap
on a.tx_id = swap.id
left join jup
on a.tx_id = jup.tx_id
left join bridge
on a.tx_id = bridge.tx_id
where (trade_source in (
    'E9bJtt4uXfzf6wuAkB44C76QGPiKBrRaQHaTEPdWiLrJ',
    '3Hy9MBHowHUmhmSP1KahEvvtC8e6DxzLGeFjuiBQwRUA',
    '4Ji3eRdwjCg2wuuJVbRBvqkPKz2xs4tchxQ4tZNhbUfs',
    'DGumNmyMhSeuNYsJLWD7VELLoENEenp99pTtDqSR9dUy',
    '2WKs6hQg3cesC2D7Hxtt878beWFezbd5dJZmFmrXumnH'
) or swap.id is not null or jup.tx_id is not null or bridge.tx_id is not null)
{% if is_incremental() %}
AND {{ incremental_predicate('a.block_time') }}
{% endif %}
group by a.tx_id, trade_source, blockchain, block_time, trader_id, amount_usd, coalesce(a.trade_souce,swap.ref,jup.ref,bridge.ref)