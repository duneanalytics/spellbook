{{ config(
        alias = alias('erc20')
        , tags = ['dunesql']
        ,partition_by=['block_month']
        ,materialized='incremental'
        ,file_format = 'delta'
        ,unique_key = ['unique_transfer_id']
        ,post_hook='{{ expose_spells(\'["ethereum", "optimism", "arbitrum", "avalanche_c", "polygon", "bnb", "gnosis", "fantom", "base", "goerli"]\',
                                    "sector",
                                    "transfers",
                                    \'["msilb7","soispoke", "dot2dotseurat", "tschubotz"]\') }}') }}


{% set evm_chains = all_evm_mainnets_testnets_chains() %} --macro: all_evm_mainnets_testnets_chains.sql


with
    sent_transfers as (
    {% for chain in evm_chains %}
        select
            '{{chain}}' AS blockchain,
            'send-' ||'{{chain}}-' || cast(evt_tx_hash as varchar) || '-' || cast (evt_index as varchar) || '-' || CAST(tr.to AS varchar) as unique_transfer_id,
            tr.to as wallet_address, tr."from" AS counterparty_address,
            contract_address as token_address,
            evt_block_time,
            evt_tx_hash,
            evt_block_number,
            t."from" AS tx_from,
            t.to AS tx_to,
            bytearray_substring(t.data,1,4) AS tx_method_id,
            cast(value as double) as amount_raw
        from
            {{ source('erc20_' + chain , 'evt_transfer') }} tr 
            INNER JOIN {{ source( chain , 'transactions') }} t
                ON tr.evt_block_time = t.block_time
                AND tr.evt_block_number = t.block_number
                AND tr.evt_tx_hash = t.hash
                {% if is_incremental() %}
                and t.block_time >= date_trunc('day', now() - interval '7' day)
                {% endif %}
        WHERE 1=1
        {% if is_incremental() %}
                and 1 = (
                        CASE
                        -- is this the first time we're running this chain?
                        WHEN NOT EXISTS (SELECT 1 FROM {{this}} WHERE blockchain = '{{chain}}') THEN 1
                        -- if not, then incremental
                        WHEN t.block_time >= date_trunc('day', now() - interval '7' day) THEN 1
                        ELSE 0
                        END
                        )
                {% endif %}

    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )

    ,
    received_transfers as (
    {% for chain in evm_chains %}
        select
            '{{chain}}' AS blockchain,
            'receive-' ||'{{chain}}-' || cast(evt_tx_hash as varchar) || '-' || cast (evt_index as varchar) || '-' || CAST(tr."from" AS varchar) as unique_transfer_id,
            tr."from" as wallet_address, tr.to AS counterparty_address,
            contract_address as token_address,
            evt_block_time,
            evt_tx_hash,
            evt_block_number,
            t."from" AS tx_from,
            t.to AS tx_to,
            bytearray_substring(t.data,1,4) AS tx_method_id,
            cast(value as double) as amount_raw
        from
            {{ source('erc20_' + chain , 'evt_transfer') }} tr 
            INNER JOIN {{ source( chain , 'transactions') }} t
                ON tr.evt_block_time = t.block_time
                AND tr.evt_block_number = t.block_number
                AND tr.evt_tx_hash = t.hash
                {% if is_incremental() %}
                and 1 = (
                        CASE
                        -- is this the first time we're running this chain?
                        WHEN NOT EXISTS (SELECT 1 FROM {{this}} WHERE blockchain = '{{chain}}') THEN 1
                        -- if not, then incremental
                        WHEN t.block_time >= date_trunc('day', now() - interval '7' day) THEN 1
                        ELSE 0
                        END
                        )
                {% endif %}
        WHERE 1=1
        {% if is_incremental() %}
        and 1 = (
                CASE
                -- is this the first time we're running this chain?
                WHEN NOT EXISTS (SELECT 1 FROM {{this}} WHERE blockchain = '{{chain}}') THEN 1
                -- if not, then incremental
                WHEN tr.evt_block_time >= date_trunc('day', now() - interval '7' day) THEN 1
                ELSE 0
                END
                )
        {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
    )

    -- Hold off until we have a way to determine wrapped assets on all chains
    -- ,
    -- deposited_weth as (
    --     select
    --         'deposit-' || cast(evt_tx_hash as varchar) || '-' || cast (evt_index as varchar) || '-' ||  CAST(dst AS varchar) as unique_transfer_id,
    --         dst as wallet_address,
    --         contract_address as token_address,
    --         evt_block_time,
    --         wad as amount_raw
    --     from
    --         {{ source('weth_optimism', 'weth9_evt_deposit') }}
    -- )

    -- ,
    -- withdrawn_weth as (
    --     select
    --         'withdraw-' || cast(evt_tx_hash as varchar) || '-' || cast (evt_index as varchar) || '-' ||  CAST(src AS varchar) as unique_transfer_id,
    --         src as wallet_address,
    --         contract_address as token_address,
    --         evt_block_time,
    --         '-' || CAST(wad AS varchar) as amount_raw
    --     from
    --         {{ source('weth_optimism', 'weth9_evt_withdrawal') }}
    -- )
    
select 
DATE_TRUNC('month', evt_block_time) AS block_month
, blockchain, unique_transfer_id, counterparty_address, token_address
, evt_block_time, evt_tx_hash, evt_block_number
, tx_from, tx_to, tx_method_id, amount_raw
from sent_transfers
union all

select
DATE_TRUNC('month', evt_block_time) AS block_month
, blockchain, unique_transfer_id, counterparty_address, token_address
, evt_block_time, evt_tx_hash, evt_block_number
, tx_from, tx_to, tx_method_id, amount_raw
from received_transfers
-- union
-- select unique_transfer_id, 'optimism' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS varchar) as amount_raw
-- from deposited_weth
-- union
-- select unique_transfer_id, 'optimism' as blockchain, wallet_address, token_address, evt_block_time, CAST(amount_raw AS varchar) as amount_raw
-- from withdrawn_weth
