{{ config(
        alias ='native_mints',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "nft",
                                    \'["umer_h_adil"]\') }}')
}}
-- to verify: should this intermediate table be exposed?


select
  'ethereum' as blockchain,
  null as project,
  null as version,
  erc721.evt_block_time as block_time,
  erc721.tokenId as token_id,
  tokens_nft.name as collection,
  prc.price * tx.value / sum(
    case
        when erc721.`from` = '0x0000000000000000000000000000000000000000' then 1
        else 0
    end
  ) over (
    partition by
      erc721.contract_address,
      erc721.evt_tx_hash
  ) / power(10, prc.decimals) as amount_usd,
  'erc721' as token_standard,
  'Single Item Trade' as trade_type,
  sum(
    case
        when erc721.`from` = '0x0000000000000000000000000000000000000000' then 1
        else 0
    end
  ) over (
    partition by
      erc721.contract_address,
      erc721.evt_tx_hash
  ) as number_of_items,
  'Buy' as trade_category,
  'Mint' as evt_type,
  erc721.`from` as seller,
  erc721.`to` as buyer,
  tx.value / sum(
    case
        when erc721.`from` = '0x0000000000000000000000000000000000000000' then 1
        else 0
    end
  ) over (
    partition by
      erc721.contract_address,
      erc721.evt_tx_hash
  ) / power(10, prc.decimals) as amount_original,
  tx.value / sum(
    case
        when erc721.`from` = '0x0000000000000000000000000000000000000000' then 1
        else 0
    end
  ) over (
    partition by
      erc721.contract_address,
      erc721.evt_tx_hash
  )  as amount_raw,
  'ETH' as currency_symbol,
  '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as currency_contract,
  erc721.contract_address as nft_contract_address,
  null as project_contract_address,
  null as aggregator_name,
  null as aggregator_address,
  erc721.evt_tx_hash as tx_hash,
  erc721.evt_block_number as block_number,
  erc721.`from` as tx_from,
  erc721.`to` as tx_to,
  erc721.contract_address || '-' || erc721.evt_tx_hash || '-' || erc721.tokenId || '-' || erc721.`from` || '-' || cast(rank(*) over (
    partition by
      erc721.contract_address,
      erc721.evt_tx_hash
    order by
      erc721.tokenId
  ) as string)
  as unique_trade_id
from
  {{ source('erc721_ethereum','evt_transfer') }} erc721
  left join {{ ref('tokens_nft') }} tokens_nft on erc721.contract_address = tokens_nft.contract_address
  inner join {{ source('ethereum','transactions') }} tx on erc721.evt_tx_hash = tx.hash
  left join {{ source('prices','usd') }} prc on prc.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
  and prc.minute = date_trunc('minute', erc721.evt_block_time)
  and prc.blockchain = 'ethereum'
where
  erc721.`from` = '0x0000000000000000000000000000000000000000'
  -- and erc721.contract_address = '0x57f1887a8bf19b14fc0df6fd9b2acc9af147ea85' -- ENS (for debugging)
  {% if is_incremental() %}
  and erc721.evt_block_time >= date_trunc("day", now() - interval '1 week')
  and tx.block_time >= date_trunc("day", now() - interval '1 week')
  and prc.minute >= date_trunc("day", now() - interval '1 week')
  {% endif %}
  {% if not is_incremental() %}
  and tx.block_number > 14801608
  {% endif %}
--limit (for debugging)
--  1000

-- to do: where to include in tranform pipeline leading to nft.mints?
--			a) in nft_mints
--			b) in nft_ethereum_events
