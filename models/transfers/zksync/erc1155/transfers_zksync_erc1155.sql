{{ config(
        
    materialized = 'incremental',
    partition_by = ['block_month'],
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'transfer_type', 'wallet_address', 'token_address', 'token_id', 'amount'],
    alias = 'erc1155',
    post_hook='{{ expose_spells(\'["zksync"]\',
                                    "sector",
                                    "transfers",
                                    \'["tomfutago", "lgingerich"]\') }}') }}

with

transfer_batch as (
    select
        t.to, 
        t."from", 
        t.contract_address, 
        t.evt_block_time,
        t.evt_tx_hash, 
        a.token_id, 
        a.amount
    from {{ source('erc1155_zksync', 'evt_transferbatch') }} t
        cross join unnest(ids, "values") as a(token_id, amount)
),

sent_transfers as (
    select
        'sent' as transfer_type,
        to as wallet_address,
        contract_address as token_address,
        cast(date_trunc('month', evt_block_time) as date) as block_month,
        evt_block_time as block_time,
        id as token_id,
        cast(value as double) as amount,
        evt_tx_hash as tx_hash
    from {{ source('erc1155_zksync', 'evt_transfersingle') }}
    where 1=1
        {% if is_incremental() %}
        and {{ incremental_predicate('evt_block_time') }}
        {% endif %}
    union all
    select
        'sent' as transfer_type,
        to as wallet_address,
        contract_address as token_address,
        cast(date_trunc('month', evt_block_time) as date) as block_month,
        evt_block_time as block_time,
        token_id,
        cast(amount as double) as amount,
        evt_tx_hash as tx_hash
    from transfer_batch
    where 1=1
        {% if is_incremental() %}
        and {{ incremental_predicate('evt_block_time') }}
        {% endif %}
),

received_transfers as (
    select
        'received' as transfer_type,
        "from" as wallet_address,
        contract_address as token_address,
        cast(date_trunc('month', evt_block_time) as date) as block_month,
        evt_block_time as block_time,
        id as token_id,
        (-1) * cast(value as double) as amount,
        evt_tx_hash as tx_hash
    from {{ source('erc1155_zksync', 'evt_transfersingle') }}
    where 1=1
        {% if is_incremental() %}
        and {{ incremental_predicate('evt_block_time') }}
        {% endif %}
    union all
    select
        'received' as transfer_type,
        "from" as wallet_address,
        contract_address as token_address,
        cast(date_trunc('month', evt_block_time) as date) as block_month,
        evt_block_time as block_time,
        token_id,
        (-1) * cast(amount as double) as amount,
        evt_tx_hash as tx_hash
    from transfer_batch
    where 1=1
        {% if is_incremental() %}
        and {{ incremental_predicate('evt_block_time') }}
        {% endif %}
)

select 'zksync' as blockchain, transfer_type, wallet_address, token_address, block_month, block_time, token_id, amount, tx_hash
from sent_transfers
union
select 'zksync' as blockchain, transfer_type, wallet_address, token_address, block_month, block_time, token_id, amount, tx_hash
from received_transfers
