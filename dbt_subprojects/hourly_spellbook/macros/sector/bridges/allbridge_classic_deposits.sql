{% macro allbridge_classic_deposits(blockchain, events = source('allbridge_' + blockchain, 'bridge_evt_sent')) %}

select
  '{{ blockchain }}' as deposit_chain,
  i.chain_id as withdrawal_chain_id,
  ci.blockchain as withdrawal_chain,
  'Allbridge' as bridge_name,
  'Classic' as bridge_version,
  evt_block_date as block_date,
  evt_block_time as block_time,
  evt_block_number as block_number,
  amount as deposit_amount_raw,
  sender,
  case
    when substr(recipient, 21) = 0x000000000000000000000000 then substr(recipient, 1, 20)
    else recipient
  end as recipient,
  substr(tokenSourceAddress, 1, 20) as deposit_token_address,
  'erc20' as deposit_token_standard,
  'erc20' as withdrawal_token_standard,
  evt_tx_from as tx_from,
  evt_tx_hash as tx_hash,
  evt_index,
  d.contract_address,
  cast(lockId as varchar) as bridge_transfer_id
from ( {{ events }} ) as d
left join {{ ref('bridges_allbridge_classic_chain_indexes') }} as ci on trim(from_utf8(d.destination)) = ci.allbridge_slug
left join {{ source('evms', 'info') }} as i on ci.blockchain = i.blockchain
where substr(tokenSourceAddress, 21) = 0x000000000000000000000000

{% endmacro %}