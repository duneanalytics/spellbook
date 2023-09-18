{% macro transfers_erc1155(blockchain, erc1155_evt_transfer_batch, erc1155_evt_transfer_single) %}

with

transfer_batch as (
  select
    t.to, t."from", t.contract_address, t.evt_block_time,
    t.evt_tx_hash, a.token_id, a.amount
  from {{ erc1155_evt_transfer_batch }} t
    cross join unnest(ids, "values") as a(token_id, amount)
),

sent_transfers as (
  select
    '{{blockchain}}' as blockchain,
    'sent' as transfer_type,
    to as wallet_address,
    contract_address as token_address,
    cast(date_trunc('month', evt_block_time) as date) as block_month,
    evt_block_time as block_time,
    id as token_id,
    cast(value as double) as amount,
    evt_tx_hash as tx_hash
  from {{ erc1155_evt_transfer_single }}
  where 1=1
    {% if is_incremental() %}
    and evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  union all
  select
    '{{blockchain}}' as blockchain,
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
    and evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

received_transfers as (
  select
    '{{blockchain}}' as blockchain,
    'received' as transfer_type,
    "from" as wallet_address,
    contract_address as token_address,
    cast(date_trunc('month', evt_block_time) as date) as block_month,
    evt_block_time as block_time,
    id as token_id,
    (-1) * cast(value as double) as amount,
    evt_tx_hash as tx_hash
  from {{ erc1155_evt_transfer_single }}
  where 1=1
    {% if is_incremental() %}
    and evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  union all
  select
    '{{blockchain}}' as blockchain,
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
    and evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

select blockchain, transfer_type, wallet_address, token_address, block_month, block_time, token_id, amount, tx_hash
from sent_transfers
union
select blockchain, transfer_type, wallet_address, token_address, block_month, block_time, token_id, amount, tx_hash
from received_transfers

{% endmacro %}
