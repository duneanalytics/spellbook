{{ config(
        alias = alias('bep20_agg_hour'),
        partition_by = ['hour'],
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key=['wallet_address', 'token_address', 'hour'],
        post_hook='{{ expose_spells(\'["bnb"]\',
                                        "sector",
                                        "transfers",
                                        \'["hosuke"]\') }}'
        )
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
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
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
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
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
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
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
        {% if is_incremental() %}
        where evt_block_time >= date_trunc('day', now() - interval '1 week')
        {% endif %}
    )
    ,
    transfers_bnb_bep20 as (
        select
            wallet_address,
            token_address,
            evt_block_time,
            amount_raw
        from sent_transfers

        union

        select
            wallet_address,
            token_address,
            evt_block_time,
            amount_raw
        from received_transfers

        union

        select
            wallet_address,
            token_address,
            evt_block_time,
            amount_raw
        from deposited_wbnb

        union

        select
            wallet_address,
            token_address,
            evt_block_time,
            amount_raw
        from withdrawn_wbnb
    )
select
    'bnb' as blockchain,
    date_trunc('hour', tr.evt_block_time) as hour,
    tr.wallet_address,
    tr.token_address,
    t.symbol,
    sum(tr.amount_raw) as amount_raw,
    sum(tr.amount_raw / power(10, t.decimals)) as amount
from transfers_bnb_bep20 tr
left join {{ ref('tokens_bnb_bep20') }} t on t.contract_address = tr.token_address
group by 1, 2, 3, 4, 5
;