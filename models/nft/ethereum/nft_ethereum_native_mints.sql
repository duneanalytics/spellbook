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
  trf.evt_block_time as block_time,
  trf.tokenId as token_id,
  tk.name as collection,
  0 as amount_usd, -- to do
  'erc721' as token_standard,
  'Single Item Trade' as trade_type, -- to verify
  1 as number_of_items, -- to verify
  'Buy' as trade_category, -- to verify
  'Mint' as evt_type,
  trf.`from` as seller,
  trf.`to` as buyer,
  0 as amount_original, -- to do
  0 as amount_raw, -- to do
  'ETH' as currency_symbol,
  '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' as currency_contract,
  trf.contract_address as nft_contract_address,
  null as project_contract_address,
  null as aggregator_name,
  null as aggregator_address,
  trf.evt_tx_hash as tx_hash,
  trf.evt_block_number as block_number,
  trf.`from` as tx_from,
  trf.`to` as tx_to,
  'TODO' as unique_trade_id -- to do
from
  erc721_ethereum.evt_Transfer as trf
  left join tokens.nft as tk on trf.contract_address = tk.contract_address
where
  `from` = '0x0000000000000000000000000000000000000000'
--limit
--  1000

-- to do: make incremental

-- to do: where to include in tranform pipelin leading to nft.mints?
--			a) in nft_mints
--			b) in nft_ethereum_events