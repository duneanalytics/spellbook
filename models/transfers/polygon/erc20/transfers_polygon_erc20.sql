{{ config(
    alias = alias('erc20'),
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['transfer_type', 'evt_tx_hash', 'evt_index', 'wallet_address'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "hosuke"]\') }}'
    )
}}

with sent_transfers as (
    select 'send'           as transfer_type,
           evt_tx_hash,
           evt_index,
           to             as wallet_address,
           contract_address as token_address,
           evt_block_time,
           value            as amount_raw
    from
        {{ source('erc20_polygon', 'evt_transfer') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),
received_transfers as (
    select 'receive'                          as transfer_type,
           evt_tx_hash,
           evt_index,
           "from"                             as wallet_address,
           contract_address                   as token_address,
           evt_block_time,
           '-' || CAST(value AS VARCHAR(100)) as amount_raw
    from
        {{ source('erc20_polygon', 'evt_transfer') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),
deposited_wmatic as (
    select 'deposit'        as transfer_type,
           evt_tx_hash,
           evt_index,
           dst              as wallet_address,
           contract_address as token_address,
           evt_block_time,
           wad              as amount_raw
    from
        {{ source('mahadao_polygon', 'wmatic_evt_deposit') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
),
withdrawn_wmatic as (
    select 'withdrawn'                      as transfer_type,
           evt_tx_hash,
           evt_index,
           src                              as wallet_address,
           contract_address                 as token_address,
           evt_block_time,
           '-' || CAST(wad AS VARCHAR(100)) as amount_raw
    from
        {{ source('mahadao_polygon', 'wmatic_evt_withdrawal') }}
    {% if is_incremental() %}
    where evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
)
    
select transfer_type,
       'polygon'                        as blockchain,
       evt_tx_hash,
       evt_index,
       wallet_address,
       token_address,
       evt_block_time,
       CAST(amount_raw AS VARCHAR(100)) as amount_raw
from sent_transfers
union
select transfer_type,
       'polygon'                        as blockchain,
       evt_tx_hash,
       evt_index,
       wallet_address,
       token_address,
       evt_block_time,
       CAST(amount_raw AS VARCHAR(100)) as amount_raw
from received_transfers
union
select transfer_type,
       'polygon'                        as blockchain,
       evt_tx_hash,
       evt_index,
       wallet_address,
       token_address,
       evt_block_time,
       CAST(amount_raw AS VARCHAR(100)) as amount_raw
from deposited_wmatic
union
select transfer_type,
       'polygon'                        as blockchain,
       evt_tx_hash,
       evt_index,
       wallet_address,
       token_address,
       evt_block_time,
       CAST(amount_raw AS VARCHAR(100)) as amount_raw
from withdrawn_wmatic
