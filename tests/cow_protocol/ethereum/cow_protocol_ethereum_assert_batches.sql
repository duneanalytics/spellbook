-- Try it out here: https://dune.com/queries/1389623
with batches_with_trades as (
    select
        s.evt_tx_hash,
        s.evt_block_time
    from {{ source('gnosis_protocol_v2_ethereum','GPv2Settlement_evt_Trade') }} t
    inner join {{ source('gnosis_protocol_v2_ethereum','GPv2Settlement_evt_Settlement') }} s
        on s.evt_tx_hash = t.evt_tx_hash
    group by s.evt_tx_hash, s.evt_block_time
)

select evt_tx_hash from batches_with_trades
where evt_tx_hash not in (select tx_hash from {{ ref('cow_protocol_ethereum_batches' )}})
-- The reference table is only refreshed once in a while,
-- so we impose a time constraint on this test.
and evt_block_time < date(now()) - interval '1' day
