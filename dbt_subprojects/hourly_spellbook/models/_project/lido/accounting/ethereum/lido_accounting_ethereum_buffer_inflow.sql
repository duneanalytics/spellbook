{{ config(
        schema='lido_accounting_ethereum',
        alias = 'buffer_inflow',

        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'append'
        , post_hook='{{ hide_spells() }}'
        )
}}

with buffer_inflow as (
    SELECT  evt_block_time as period, amount as amount,0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as token, evt_tx_hash, date_trunc('day', evt_block_time) as day
    FROM {{source('lido_ethereum','steth_evt_Submitted')}}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    union all

    SELECT evt_block_time, amount, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, evt_tx_hash, date_trunc('day', evt_block_time) as day
    FROM {{source('lido_ethereum','steth_evt_ELRewardsReceived')}}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}

    union all

    SELECT evt_block_time, amount , 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2, evt_tx_hash, date_trunc('day', evt_block_time) as day
    FROM {{source('lido_ethereum','steth_evt_WithdrawalsReceived')}}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT period, amount, token, evt_tx_hash, day
FROM buffer_inflow b
{% if is_incremental() %}
-- append-only dedup: drop rows already inserted by a previous run inside the
-- incremental window (no event index in the output, so a merge unique_key would
-- collapse legitimately duplicated events within one tx)
WHERE not exists (
    select 1
    from {{ this }} t
    where t.period = b.period
      and t.amount = b.amount
      and t.token = b.token
      and t.evt_tx_hash = b.evt_tx_hash
      and t.day = b.day
)
{% endif %}
