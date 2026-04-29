{{
  config(
    schema = 'tokens_xrpl',
    alias = 'net_transfers_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
  )
}}

with raw_transfers as (
  select
    blockchain,
    block_date,
    "from" as address,
    'sent' as transfer_direction,
    sum(amount_usd) * -1 as transfer_amount_usd
  from {{ ref('tokens_xrpl_transfers') }}
  where blockchain = 'xrpl'
    and amount_usd is not null
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
  group by
    blockchain,
    block_date,
    "from",
    'sent'

  union all

  select
    blockchain,
    block_date,
    to as address,
    'received' as transfer_direction,
    sum(amount_usd) as transfer_amount_usd
  from {{ ref('tokens_xrpl_transfers') }}
  where blockchain = 'xrpl'
    and amount_usd is not null
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
  group by
    blockchain,
    block_date,
    to,
    'received'
),
transfers_amount as (
  select
    blockchain,
    block_date,
    cast(address as varchar) as address_owner,
    sum(case when transfer_direction = 'sent' then transfer_amount_usd else 0 end) as transfer_amount_usd_sent,
    sum(case when transfer_direction = 'received' then transfer_amount_usd else 0 end) as transfer_amount_usd_received
  from raw_transfers
  group by
    blockchain,
    block_date,
    cast(address as varchar)
),
net_transfers as (
  select
    blockchain,
    block_date,
    address_owner,
    sum(coalesce(transfer_amount_usd_sent, 0)) as transfer_amount_usd_sent,
    sum(coalesce(transfer_amount_usd_received, 0)) as transfer_amount_usd_received,
    sum(coalesce(transfer_amount_usd_received, 0)) + sum(coalesce(transfer_amount_usd_sent, 0)) as net_transfer_amount_usd
  from transfers_amount
  group by
    blockchain,
    block_date,
    address_owner
)
select
  blockchain,
  block_date,
  sum(net_transfer_amount_usd) as net_transfer_amount_usd
from net_transfers
where net_transfer_amount_usd > 0
group by
  blockchain,
  block_date
