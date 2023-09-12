{{ 
    config(
        tags = ['dunesql'],
        alias = alias('approvals'),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "nft",
                                    \'["tomfutago"]\') }}'
    )
}}

select
  'celo' as blockchain,
  evt_block_time as block_time,
  date_trunc('day', evt_block_time) as block_date,
  evt_block_number as block_number,
  owner as address,
  'erc721' as token_standard,
  cast(false as boolean) as approval_for_all,
  contract_address,
  tokenId as token_id,
  approved, --cast(approved as boolean) as approved,
  evt_tx_hash as tx_hash,
  evt_index
from {{ source('erc721_celo', 'evt_Approval') }}
{% if is_incremental() %}
where evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

union all

select
  'celo' as blockchain,
  evt_block_time as block_time,
  date_trunc('day', evt_block_time) as block_date,
  evt_block_number as block_number,
  owner as address,
  'erc721' as token_standard,
  cast(true as boolean) as approval_for_all,
  contract_address,
  cast(null as uint256) as token_id,
  approved, --cast(approved as boolean) as approved,
  evt_tx_hash as tx_hash,
  evt_index
from {{ source('erc721_celo', 'evt_ApprovalForAll') }}
from erc721_celo.evt_ApprovalForAll
{% if is_incremental() %}
where evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

union all

select
  'celo' as blockchain,
  evt_block_time as block_time,
  date_trunc('day', evt_block_time) as block_date,
  evt_block_number as block_number,
  owner as address,
  'erc1155' as token_standard,
  cast(true as boolean) as approval_for_all,
  contract_address,
  cast(null as uint256) as token_id,
  approved, --cast(approved as boolean) as approved,
  evt_tx_hash as tx_hash,
  evt_index
from {{ source('erc1155_celo', 'evt_ApprovalForAll') }}
from erc721_celo.evt_ApprovalForAll
{% if is_incremental() %}
where evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
