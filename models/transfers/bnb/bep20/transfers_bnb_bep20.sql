{{ config(
    materialized='view',
    alias='bep20',
    post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "transfers",
                                    \'["hosuke"]\') }}')
}}


with
    sent_transfers as (
        select
            `to` as wallet_address,
            contract_address as token_address,
            evt_block_time,
            value as amount_raw
        from
            {{ source('erc20_bnb', 'evt_Transfer') }}
    )

    ,
    received_transfers as (
        select
            `from` as wallet_address,
            contract_address as token_address,
            evt_block_time,
            - value as amount_raw
        from
            {{ source('erc20_bnb', 'evt_Transfer') }}
    )

    ,
    deposited_wbnb as (
        select
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
            src as wallet_address,
            contract_address as token_address,
            evt_block_time,
            - wad as amount_raw
        from
            {{ source('bnb_bnb', 'WBNB_evt_Withdrawal') }}
    )

select
    'bnb' as blockchain,
    wallet_address,
    token_address,
    evt_block_time,
    amount_raw
from sent_transfers

union

select
    'bnb' as blockchain,
    wallet_address,
    token_address,
    evt_block_time,
    amount_raw
from received_transfers

union

select
    'bnb' as blockchain,
    wallet_address,
    token_address,
    evt_block_time,
    amount_raw
from deposited_wbnb

union

select
    'bnb' as blockchain,
    wallet_address,
    token_address,
    evt_block_time,
    amount_raw
from withdrawn_wbnb