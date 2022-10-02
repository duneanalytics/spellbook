{{ config(
    materialized='view',
    alias='bnb',
    post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "transfers",
                                    \'["hosuke"]\') }}')
}}


with
    sent_transactions as (
        select
            'send' || '-' || hash || '-' || index || '-' || `to` as unique_transfer_id,
            `to` as wallet_address,
            block_time as evt_block_time,
            value as amount_raw
        from
            {{ source('bnb', 'transactions') }}
    )

    ,
    received_transactions as (
        select
        'receive' || '-' || hash || '-' || index || '-' || `from` as unique_transfer_id,
        `from` as wallet_address,
        block_time as evt_block_time,
        - value as amount_raw
        from
            {{ source('bnb', 'transactions') }}
    )

    ,
    sent_transfers as (
        select
            'send' || '-' || evt_tx_hash || '-' || evt_index || '-' || dst as unique_transfer_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            wad as amount_raw
        from
            {{ source('bnb_bnb', 'WBNB_evt_Transfer') }}
    )

    ,
    received_transfers as (
        select
            'receive' || '-' || evt_tx_hash || '-' || evt_index || '-' || dst as unique_transfer_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            - wad as amount_raw
        from
            {{ source('bnb_bnb', 'WBNB_evt_Transfer') }}
    )

    ,
    deposited_wbnb as (
        select
            'deposit' || '-' || evt_tx_hash || '-' || evt_index || '-' || dst as unique_transfer_id,
            dst as wallet_address,
            contract_address as token_address,
            evt_block_time,
            wad as amount_raw
        from
            {{ source('bnb_bnb', 'WBNB_evt_Deposit') }}
    )

    ,
    withdrawn_wbnb as (
        select
            'withdrawn' || '-' || evt_tx_hash || '-' || evt_index || '-' || src as unique_transfer_id,
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            - wad as amount_raw
        from
            {{ source('bnb_bnb', 'WBNB_evt_Withdrawal') }}
    )

select
    unique_transfer_id,
    'bnb' as blockchain,
    wallet_address,
    evt_block_time,
    amount_raw
from sent_transfers

union

select
    unique_transfer_id,
    'bnb' as blockchain,
    wallet_address,
    evt_block_time,
    amount_raw
from received_transfers

union

select
    unique_transfer_id,
    'bnb' as blockchain,
    wallet_address,
    evt_block_time,
    amount_raw
from deposited_wbnb

union

select
    unique_transfer_id,
    'bnb' as blockchain,
    wallet_address,
    token_address,
    evt_block_time,
    amount_raw
from withdrawn_wbnb