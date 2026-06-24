{{ config(
        schema='lido_accounting_ethereum',
        alias = 'buffer_outflow',

        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'append'
        , post_hook='{{ hide_spells() }}'
        )
}}

SELECT evt_block_time as period, amountOfETHLocked as amount, 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 AS token,  evt_tx_hash, date_trunc('day', evt_block_time) as day
FROM {{source('lido_ethereum','WithdrawalQueueERC721_evt_WithdrawalsFinalized')}} w
{% if is_incremental() %}
WHERE {{ incremental_predicate('evt_block_time') }}
-- append-only dedup: drop rows already inserted by a previous run inside the
-- incremental window (no event index in the output, so a merge unique_key would
-- collapse legitimately duplicated events within one tx)
AND not exists (
    select 1
    from {{ this }} t
    where t.period = w.evt_block_time
      and t.amount = w.amountOfETHLocked
      and t.token = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
      and t.evt_tx_hash = w.evt_tx_hash
      and t.day = date_trunc('day', w.evt_block_time)
)
{% endif %}
