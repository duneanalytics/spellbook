{{ 
    config(
        tags = ['dunesql'],
        alias = alias('transfers'),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index', 'contract_address', 'token_id'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "nft",
                                    \'["tomfutago"]\') }}'
    )
}}

select
    'celo' as blockchain,
    t.evt_block_time as block_time,
    date_trunc('day', t.evt_block_time) as block_date,
    t.evt_block_number as block_number,
    'erc721' as token_standard,
    'single' as transfer_type,
    t.evt_index,
    t.contract_address,
    t.tokenId as token_id,
    uint256 '1' as amount,
    t."from",
    t.to,
    tx."from" as executed_by,
    t.evt_tx_hash as tx_hash
    --, 'celo' || t.evt_tx_hash || '-erc721-' || t.contract_address || '-' || t.tokenId || '-' || t."from" || '-' || t.to || '-' || '1' || '-' || t.evt_index as unique_transfer_id
from {{ source('erc721_celo', 'evt_transfer') }} t
    {% if is_incremental() %}
    left join {{ this }} anti_table on t.evt_tx_hash = anti_table.tx_hash
    {% endif %}
    join {{ source('celo', 'transactions') }} tx ON tx.block_time = t.evt_block_time and tx.hash = t.evt_tx_hash
        {% if is_incremental() %}
        and tx.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
{% if is_incremental() %}
where t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    and anti_table.tx_hash is null
{% endif %}

union all

select
    'celo' as blockchain,
    t.evt_block_time as block_time,
    date_trunc('day', t.evt_block_time) as block_date,
    t.evt_block_number as block_number,
    'erc1155' as token_standard,
    'single' as transfer_type,
    t.evt_index,
    t.contract_address,
    t.id as token_id,
    t.value as amount,
    t."from",
    t.to,
    tx."from" as executed_by,
    t.evt_tx_hash as tx_hash
    --, 'celo' || t.evt_tx_hash || '-erc721-' || t.contract_address || '-' || t.tokenId || '-' || t."from" || '-' || t.to || '-' || '1' || '-' || t.evt_index as unique_transfer_id
from {{ source('erc1155_celo', 'evt_transfersingle') }} t
    {% if is_incremental() %}
    left join {{ this }} anti_table on t.evt_tx_hash = anti_table.tx_hash
    {% endif %}
    join {{ source('celo', 'transactions') }} tx ON tx.block_time = t.evt_block_time and tx.hash = t.evt_tx_hash
        {% if is_incremental() %}
        and tx.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
{% if is_incremental() %}
where t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    and anti_table.tx_hash is null
{% endif %}

union all

select
    'celo' as blockchain,
    t.evt_block_time as block_time,
    date_trunc('day', t.evt_block_time) as block_date,
    t.evt_block_number as block_number,
    'erc1155' as token_standard,
    'batch' as transfer_type,
    t.evt_index,
    t.contract_address,
    a.token_id,
    a.amount,
    t."from",
    t.to,
    tx."from" as executed_by,
    t.evt_tx_hash as tx_hash
    --, 'celo'  || t.evt_tx_hash || '-erc1155-' || t.contract_address || '-' || t.ids_and_count.ids || '-' || t.from || '-' || t.to || '-' || t.ids_and_count.values || '-' || t.evt_index as unique_transfer_id
from {{ source('erc1155_celo', 'evt_transferbatch') }} t
    cross join unnest(ids, "values") as a(token_id, amount)
    {% if is_incremental() %}
    left join {{ this }} anti_table on t.evt_tx_hash = anti_table.tx_hash
    {% endif %}
    join {{ source('celo', 'transactions') }} tx ON tx.block_time = t.evt_block_time and tx.hash = t.evt_tx_hash
        {% if is_incremental() %}
        and tx.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
where 1=1
    and cast(a.amount as double) > 0
    {% if is_incremental() %}
    and anti_table.tx_hash is null
    {% endif %}
