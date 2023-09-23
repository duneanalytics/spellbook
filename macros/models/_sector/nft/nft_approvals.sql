{% macro nft_approvals(blockchain, erc721_approval, erc721_approval_all, erc1155_approval_all ) %}
{%- set token_standard_721 = 'bep721' if blockchain == 'bnb' else 'erc721' -%}
{%- set token_standard_1155 = 'bep1155' if blockchain == 'bnb' else 'erc1155' -%}

select
  '{{blockchain}}' as blockchain,
  evt_block_time as block_time,
  cast(date_trunc('day', evt_block_time) as date) as block_date,
  evt_block_number as block_number,
  owner as address,
  '{{token_standard_721}}' as token_standard,
  cast(false as boolean) as approval_for_all,
  contract_address,
  tokenId as token_id,
  cast(true as boolean) as approved,
  approved as operator,
  evt_tx_hash as tx_hash,
  evt_index
from {{ erc721_approval }}
{% if is_incremental() %}
where evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

union all

select
  '{{blockchain}}' as blockchain,
  evt_block_time as block_time,
  cast(date_trunc('day', evt_block_time) as date) as block_date,
  evt_block_number as block_number,
  owner as address,
  '{{token_standard_721}}' as token_standard,
  cast(true as boolean) as approval_for_all,
  contract_address,
  cast(null as uint256) as token_id,
  approved,
  operator,
  evt_tx_hash as tx_hash,
  evt_index
from {{ erc721_approval_all }}
{% if is_incremental() %}
where evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

union all

select
  '{{blockchain}}' as blockchain,
  evt_block_time as block_time,
  cast(date_trunc('day', evt_block_time) as date) as block_date,
  evt_block_number as block_number,
  account as address,
  '{{token_standard_1155}}' as token_standard,
  cast(true as boolean) as approval_for_all,
  contract_address,
  cast(null as uint256) as token_id,
  approved,
  operator,
  evt_tx_hash as tx_hash,
  evt_index
from {{ erc1155_approval_all }}
{% if is_incremental() %}
where evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

{% endmacro %}
