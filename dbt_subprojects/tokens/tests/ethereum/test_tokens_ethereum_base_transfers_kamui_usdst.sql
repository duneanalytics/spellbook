-- Kamui USDst settles share burns before emitting Withdraw in a later claim tx.
-- Regression: settlement tx 0xa1950e7f9c084a41eb6513905ddd0d8af6d45414b62357ad38b25915734acdf2
-- log 456 burns 300,000 shares; claim tx
-- 0xf9fdd0b0a82f1556c0c6fe66fce055f19189f1724901581267bc3c3a61cd11a6
-- log 1082 must not synthesize another burn.
-- Use the normal processing window so this also runs against bounded CI builds.

with vault_transfers as (
    select
        block_date
        , tx_hash
        , evt_index
        , "from"
        , "to"
        , amount_raw
    from {{ ref('tokens_ethereum_base_transfers') }}
    where contract_address = 0xcda323c2df692d989b24ba51d0acca924cf9a344
        and {{ incremental_predicate('block_time') }}
)

, synthetic_claim_burns as (
    select
        t.block_date
        , t.tx_hash
        , t.evt_index
        , 'synthetic_claim_burn' as failure
    from vault_transfers as t
    inner join {{ source('erc4626_ethereum', 'evt_withdraw') }} as w
        on w.evt_tx_hash = t.tx_hash
        and w.evt_index = t.evt_index
        and w.contract_address = 0xcda323c2df692d989b24ba51d0acca924cf9a344
        and {{ incremental_predicate('w.evt_block_time') }}
    where t."to" = 0x0000000000000000000000000000000000000000
)

, missing_real_transfers as (
    select
        cast(date_trunc('day', e.evt_block_time) as date) as block_date
        , e.evt_tx_hash as tx_hash
        , e.evt_index
        , 'missing_real_transfer' as failure
    from {{ source('erc20_ethereum', 'evt_Transfer') }} as e
    left join vault_transfers as t
        on t.tx_hash = e.evt_tx_hash
        and t.evt_index = e.evt_index
        and t."from" = e."from"
        and t."to" = e."to"
        and t.amount_raw = e.value
    where e.contract_address = 0xcda323c2df692d989b24ba51d0acca924cf9a344
        and {{ incremental_predicate('e.evt_block_time') }}
        and t.tx_hash is null
)

select block_date, tx_hash, evt_index, failure from synthetic_claim_burns
union all
select block_date, tx_hash, evt_index, failure from missing_real_transfers
