{{ config(
        alias ='native_mints',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
        unique_key='unique_trade_id',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "nft",
                                    \'["umer_h_adil"]\') }}')
}}

with ercs as (
select
  evt_block_time,
  evt_block_number,
  evt_tx_hash,
  tokenId,
  from,
  to,
  contract_address,
  'erc721' as token_standard,
  'Single Item Trade' as trade_type,
  1 as number_of_items
 from {{ source('erc721_ethereum','evt_transfer') }}
 where from = '0x0000000000000000000000000000000000000000'
	union
select
  evt_block_time,
  evt_block_number,
  evt_tx_hash,
  id as tokenId,
  from,
  to,
  contract_address,
  'erc1155' as token_standard,
  -- with an TransferSingle event only one item type is traded
  'Single Item Trade' as trade_type,
  -- of that item type any number of items can be traded
  value as number_of_items
 from {{ source('erc1155_ethereum','evt_transfersingle') }}
 where from = '0x0000000000000000000000000000000000000000'
)
select
  'ethereum' as blockchain,
  ec.namespace as project,
  null as version,
  ercs.evt_block_time as block_time,
  ercs.tokenId as token_id,
  tokens_nft.name as collection,
  prc.price * tx.value / sum(
    case
        when ercs.`from` = '0x0000000000000000000000000000000000000000' then 1
        else 0
    end
  ) over (
    partition by
      ercs.contract_address,
      ercs.evt_tx_hash
  ) / power(10, prc.decimals) as amount_usd,
  token_standard,
  trade_type,
  sum(
    case
        when ercs.`from` = '0x0000000000000000000000000000000000000000' then 1
        else 0
    end
  ) over (
    partition by
      ercs.contract_address,
      ercs.evt_tx_hash
  ) as number_of_items,
  'Buy' as trade_category,
  'Mint' as evt_type,
  ercs.`from` as seller,
  ercs.`to` as buyer,
  tx.value / sum(
    case
        when ercs.`from` = '0x0000000000000000000000000000000000000000' then 1
        else 0
    end
  ) over (
    partition by
      ercs.contract_address,
      ercs.evt_tx_hash
  ) / power(10, prc.decimals) as amount_original,
  tx.value / sum(
    case
        when ercs.`from` = '0x0000000000000000000000000000000000000000' then 1
        else 0
    end
  ) over (
    partition by
      ercs.contract_address,
      ercs.evt_tx_hash
  )  as amount_raw,
  'ETH' as currency_symbol,
  '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as currency_contract,
  ercs.contract_address as nft_contract_address,
  ercs.to as project_contract_address,
  null as aggregator_name,
  null as aggregator_address,
  ercs.evt_tx_hash as tx_hash,
  ercs.evt_block_number as block_number,
  ercs.`from` as tx_from,
  ercs.`to` as tx_to,
  ercs.contract_address || '-' || ercs.evt_tx_hash || '-' || ercs.tokenId || '-' || ercs.`from` || '-' || cast(rank(*) over (
    partition by
      ercs.contract_address,
      ercs.evt_tx_hash
    order by
      ercs.tokenId
  ) as string)
  as unique_trade_id
from
  ercs
  left join {{ ref('tokens_nft') }} tokens_nft on ercs.contract_address = tokens_nft.contract_address
  inner join {{ source('ethereum','transactions') }} tx on ercs.evt_tx_hash = tx.hash
  left join {{ source('prices','usd') }} prc on prc.contract_address = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
  and prc.minute = date_trunc('minute', ercs.evt_block_time)
  and prc.blockchain = 'ethereum'
  left join {{ source('ethereum','contracts') }} ec ON ec.address = ercs.to
where
  -- We're intersted in collectible NFTs (e.g. BAYC), not functional NFTs (e.g. Uniswap LP), so we exclude NFTs originated in DeFi 
  ercs.to not in (select address from {{ ref('addresses_ethereum_defi') }})
  {% if is_incremental() %}
  and ercs.evt_block_time >= date_trunc("day", now() - interval '1 week')
  and tx.block_time >= date_trunc("day", now() - interval '1 week')
  and prc.minute >= date_trunc("day", now() - interval '1 week')
  {% endif %}
  {% if not is_incremental() %}
  and tx.block_number > 14801608
  {% endif %}
--limit -- (for debugging)
--  10
