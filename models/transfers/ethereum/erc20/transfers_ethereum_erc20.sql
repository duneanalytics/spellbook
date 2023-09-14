{{ config(
        tags = ['dunesql'],
        materialized='incremental',
        partition_by = ['block_month'],
        file_format='delta',
        incremental_strategy='merge',
        unique_key=['unique_transfer_id', ],
        alias = alias('erc20'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke","dot2dotseurat"]\') }}') }}

with
    sent_transfers as (
        select
            'send-' || CAST(evt_tx_hash as varchar) || '-' || CAST(evt_index AS VARCHAR) || '-' || CAST("to" AS VARCHAR) as unique_transfer_id,
            "to" as wallet_address,
            contract_address as token_address,
            evt_block_time,
            cast(value as double) as amount_raw
        from
            {{ source('erc20_ethereum', 'evt_transfer') }}
        {% if is_incremental %}
            where evt_block_time >= date_trunc('day', now() - interval '3' day)
        {% endif %}
    )

    ,
    received_transfers as (
        select
        'receive-' || CAST(evt_tx_hash AS VARCHAR) || '-' || CAST(evt_index AS VARCHAR) || '-' || CAST("from" AS VARCHAR) as unique_transfer_id,
        "from" as wallet_address,
        contract_address as token_address,
        evt_block_time,
        - cast(value as double) as amount_raw
        from
            {{ source('erc20_ethereum', 'evt_transfer') }}
        {% if is_incremental %}
            where evt_block_time >= date_trunc('day', now() - interval '3' day)
        {% endif %}
    )

    ,
    deposited_weth as (
        select
            'deposit-' || CAST(evt_tx_hash AS VARCHAR) || '-' || CAST(evt_index AS VARCHAR) || '-' || CAST(dst AS VARCHAR) as unique_transfer_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            cast(wad as double) as amount_raw
        from
            {{ source('zeroex_ethereum', 'weth9_evt_deposit') }}
        {% if is_incremental %}
            where evt_block_time >= date_trunc('day', now() - interval '3' day)
        {% endif %}
    )

    ,
    withdrawn_weth as (
        select
            'withdrawn-' || CAST(evt_tx_hash AS VARCHAR) || '-' || CAST(evt_index AS VARCHAR) || '-' || CAST(src AS VARCHAR) as unique_transfer_id,
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            - cast(wad as double) as amount_raw
        from
            {{ source('zeroex_ethereum', 'weth9_evt_withdrawal') }}
        {% if is_incremental %}
            where evt_block_time >= date_trunc('day', now() - interval '3' day)
        {% endif %}
    )
    
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, amount_raw, CAST(date_trunc('month', evt_block_time) as date) as block_month
from sent_transfers
union
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, amount_raw, CAST(date_trunc('month', evt_block_time) as date) as block_month
from received_transfers
union
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, amount_raw,CAST(date_trunc('month', evt_block_time) as date) as block_month
from deposited_weth
union
select unique_transfer_id, 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, amount_raw,CAST(date_trunc('month', evt_block_time) as date) as block_month
from withdrawn_weth
