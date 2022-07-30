{{ config(alias='erc721_transfers',
          materialized ='incremental',
          file_format ='delta',
          incremental_strategy='merge',
          unique_key='unique_tx_id',
          partition='day')
}}

with
    received_transfers as (
        select 'receive' || '-' ||  evt_tx_hash || '-' || evt_index || '-' || `to` as unique_tx_id,
            to as wallet_address,
            contract_address as token_address,
            evt_block_time,
            tokenId,
            1 as amount
        from
            {{ source('erc721_ethereum', 'evt_transfer') }}
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where date_trunc('day', tr.evt_block_time) > now() - interval 2 days
        {% endif %}
    )

    ,
    sent_transfers as (
        select 'send' || '-' || evt_tx_hash || '-' || evt_index || '-' || `from` as unique_tx_id,
            from as wallet_address,
            contract_address as token_address,
            evt_block_time,
            tokenId,
            -1 as amount
        from
            {{ source('erc721_ethereum', 'evt_transfer') }}
        {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where date_trunc('day', tr.evt_block_time) > now() - interval 2 days
        {% endif %}
    )

select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id, date_trunc('day', evt_block_time) as day
from sent_transfers
union
select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id, date_trunc('day', evt_block_time) as day
from received_transfers
