{{
    config(
        tags=["prod_exclude"],
        materialized="view",
        schema="integration_test",
        alias="test_view",
    )
}}

with
    erc1155_ids_batch as (
        select
            *,
            cast(evt_tx_hash as varchar)
            || '-'
            || cast(
                row_number() over (
                    partition by evt_tx_hash, ids order by ids
                ) as varchar
            ) as unique_transfer_id
        from {{ source("erc1155_ethereum", "evt_transferbatch") }}
        cross join unnest(ids) as _u(explode_id)
        limit 100
    ),

    erc1155_values_batch as (
        select
            *,
            cast(evt_tx_hash as varchar)
            || '-'
            || cast(
                row_number() over (
                    partition by evt_tx_hash, ids order by ids
                ) as varchar
            ) as unique_transfer_id
        from {{ source("erc1155_ethereum", "evt_transferbatch") }}
        cross join unnest("values") as _u(explode_value)
        limit 100
    ),

    erc1155_transfers_batch as (
        select distinct
            erc1155_ids_batch.explode_id,
            cast(erc1155_values_batch.explode_value as double) as explode_value,
            erc1155_ids_batch.evt_tx_hash,
            erc1155_ids_batch.to,
            erc1155_ids_batch."from",
            erc1155_ids_batch.contract_address,
            erc1155_ids_batch.evt_index,
            erc1155_ids_batch.evt_block_time
        from erc1155_ids_batch
        join
            erc1155_values_batch
            on erc1155_ids_batch.unique_transfer_id
            = erc1155_values_batch.unique_transfer_id
        limit 100
    ),

    sent_transfers as (
        select
            evt_tx_hash,
            cast(evt_tx_hash as varchar)
            || '-'
            || cast(evt_index as varchar)
            || '-'
            || cast(to as varchar) as unique_tx_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            id as tokenid,
            cast(value as double) as amount
        from {{ source("erc1155_ethereum", "evt_transfersingle") }} single
        union all
        select
            evt_tx_hash,
            cast(evt_tx_hash as varchar)
            || '-'
            || cast(evt_index as varchar)
            || '-'
            || cast(to as varchar) as unique_tx_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            explode_id as tokenid,
            cast(explode_value as double) as amount
        from erc1155_transfers_batch
        limit 100
    ),

    received_transfers as (
        select
            evt_tx_hash,
            cast(evt_tx_hash as varchar)
            || '-'
            || cast(evt_index as varchar)
            || '-'
            || cast(to as varchar) as unique_tx_id,
            "from" as wallet_address,
            contract_address as token_address,
            evt_block_time,
            id as tokenid, - cast(value as double) as amount
        from {{ source("erc1155_ethereum", "evt_transfersingle") }}
        union all
        select
            evt_tx_hash,
            cast(evt_tx_hash as varchar)
            || '-'
            || cast(evt_index as varchar)
            || '-'
            || cast(to as varchar) as unique_tx_id,
            "from" as wallet_address,
            contract_address as token_address,
            evt_block_time,
            explode_id as tokenid, - cast(explode_value as double) as amount
        from erc1155_transfers_batch
        limit 100
    )

select
    'ethereum' as blockchain,
    wallet_address,
    token_address,
    evt_block_time,
    tokenid,
    amount,
    evt_tx_hash,
    unique_tx_id
from sent_transfers
union all
select
    'ethereum' as blockchain,
    wallet_address,
    token_address,
    evt_block_time,
    tokenid,
    amount,
    evt_tx_hash,
    unique_tx_id
from received_transfers
limit
    100

    -- test test
    
