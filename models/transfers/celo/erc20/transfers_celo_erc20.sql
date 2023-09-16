{{ 
    config(
        tags = ['dunesql'],
        schema = 'transfers_celo',
        alias = alias('erc20'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'transfer_type', 'evt_index', 'wallet_address'],
        post_hook = '{{ expose_spells(\'["celo"]\',
                                    "sector",
                                    "transfers",
                                    \'["soispoke", "dot2dotseurat", "tschubotz", "tomfutago"]\') }}'
    )
}}

with
  sent_transfers as (
    select
      'sent' as transfer_type,
      to as wallet_address,
      contract_address as token_address,
      evt_block_time as block_time,
      cast(date_trunc('month', evt_block_time) as date) as block_month,
      cast(value as double) as amount_raw,
      evt_index,
      evt_tx_hash as tx_hash
    from {{ source('erc20_celo', 'evt_transfer') }}
    where 1=1
      and contract_address <> 0x471EcE3750Da237f93B8E339c536989b8978a438 -- CELO native asset
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and evt_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
  ),
  
  received_transfers as (
    select
      'received' as transfer_type,
      "from" as wallet_address,
      contract_address as token_address,
      evt_block_time as block_time,
      cast(date_trunc('month', evt_block_time) as date) as block_month,
      (-1) * cast(value as double) as amount_raw,
      evt_index,
      evt_tx_hash as tx_hash
    from {{ source('erc20_celo', 'evt_transfer') }}
    where 1=1
      and contract_address <> 0x471EcE3750Da237f93B8E339c536989b8978a438 -- CELO native asset
      {% if is_incremental() %} -- this filter will only be applied on an incremental run
      and evt_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
  )

select 'celo' as blockchain, transfer_type, wallet_address, token_address, block_time, block_month, amount_raw, evt_index, tx_hash
from sent_transfers
union
select 'celo' as blockchain, transfer_type, wallet_address, token_address, block_time, block_month, amount_raw, evt_index, tx_hash
from received_transfers
