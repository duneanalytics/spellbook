{{ config(
        alias ='events',
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
  0 as amount_usd, -- to do
  'erc721' as token_standard,
  'Single Item Trade' as trade_type,
  1 as number_of_items,
  'Buy' as trade_category, -- to verify
  'Mint' as evt_type,
  erc721.`from` as seller,
  erc721.`to` as buyer,
  0 as amount_original, -- to do
  0 as amount_raw, -- to do
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
  erc721.contract_address || '-' || erc721.evt_tx_hash || '-' || erc721.tokenId || '-' || erc721.`from` as unique_trade_id -- to verify
from
  {{ source('erc721_ethereum','evt_transfer') }} erc721
  left join {{ ref('tokens_nft') }} tokens_nft on erc721.contract_address = tokens_nft.contract_address
where
  `from` = '0x0000000000000000000000000000000000000000'
--limit
--  1000

-- to do: make incremental

-- to do: where to include in tranform pipelin leading to nft.mints?
--			a) in nft_mints
--			b) in nft_ethereum_events

-- to do: get amount_original, amount_raw
--			-> join on eth.tx; then amount_raw = value/(#mints in tx) ? 
--			-> amount_original = amount_raw / 10e18 ?

-- to do: get amount_usd
--			-> join on prices table by block_time?
