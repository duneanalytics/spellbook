{% macro transfers_erc721(blockchain, erc721_evt_transfer) %}

with

sent_transfers as (
  select
    '{{blockchain}}' as blockchain,
    'sent' as transfer_type,
    to as wallet_address,
    contract_address as token_address,
    cast(date_trunc('month', evt_block_time) as date) as block_month,
    evt_block_time as block_time,
    tokenId as token_id,
    1 as amount,
    evt_tx_hash as tx_hash
  from {{ erc721_evt_transfer }}
  where 1=1
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
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
    tokenId as token_id,
    -1 as amount,
    evt_tx_hash as tx_hash
  from {{ erc721_evt_transfer }}
  where 1=1
    {% if is_incremental() %} -- this filter will only be applied on an incremental run
    and evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

select blockchain, transfer_type, wallet_address, token_address, block_month, block_time, token_id, amount, tx_hash
from sent_transfers
union
select blockchain, transfer_type, wallet_address, token_address, block_month, block_time, token_id, amount, tx_hash
from received_transfers

{% endmacro %}
