{{ config(
    alias = 'trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'unique_trade_id'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                            "project",
                            "seaport",
                            \'["sohawk"]\') }}'
    )
}}

-- base페어에 price, platform, creator를 붙여봅시다.
with iv_volume as (
    select block_time
          ,tx_hash
          ,evt_index
          ,max(token_contract_address::text) as token_contract_address 
          ,sum(case when is_price then original_amount end) as price_amount_raw
          ,sum(case when is_platform_fee then original_amount end) as platform_fee_amount_raw
          ,max(case when is_platform_fee then receiver::text end) as platform_fee_receiver
          ,sum(case when is_creator_fee then original_amount end) as creator_fee_amount_raw
          ,sum(case when is_creator_fee and creator_fee_idx = 1 then original_amount end) as creator_fee_amount_1_raw
          ,sum(case when is_creator_fee and creator_fee_idx = 2 then original_amount end) as creator_fee_amount_2_raw
          ,sum(case when is_creator_fee and creator_fee_idx = 3 then original_amount end) as creator_fee_amount_3_raw
          ,sum(case when is_creator_fee and creator_fee_idx = 4 then original_amount end) as creator_fee_amount_4_raw
          ,max(case when is_creator_fee and creator_fee_idx = 1 then receiver::text end) as creator_fee_receiver_1_raw
          ,max(case when is_creator_fee and creator_fee_idx = 2 then receiver::text end) as creator_fee_receiver_2_raw
          ,max(case when is_creator_fee and creator_fee_idx = 3 then receiver::text end) as creator_fee_receiver_3_raw
          ,max(case when is_creator_fee and creator_fee_idx = 4 then receiver::text end) as creator_fee_receiver_4_raw
      from dune_user_generated.sohwak_os_s10_base_pair_3 a
     where 1=1
       and eth_erc_idx > 0
    --   and tx_hash = '\xa9cb862c5a172319dd403f72e5ba806708d7bb8774a2f61d55ea169d27923a19' -- multi buy
    --   and tx_hash = '\x067494c3c2e1dfc68d4806e51ebb76d89f7be369f752fcaa1df4e7d91acbbda8' -- bundle trade
    --   and tx_hash = '\x08eba6fac7c403068b8ed13bd2fc37e9717a29929581a66c9e88ca88fa206a3d' -- multitrade
    --   and tx_hash = '\x0001e6fd67badfda25f85364215572173b1eff29ff11362987562b23b730fb47' -- offer_accepted, 1 nft
    --   and tx_hash = '\x8cd79572bb14ee10a07a149644b4e5f37f62cb5d029ea23713107094f53ff68c' -- offer_accepted, 20 nft, weth
    --   and tx_hash = '\x0b836ca871e15048c48c4143c243d7c7f35323fd20ecd394ce004fe48101edc6' -- offer_accepted, 2x creator fee 
    --   and tx_hash = '\xd95bfa3251c5971ca7dec1b1cbde9ffb8637074810fafbdda4cfacf32e2698c9' -- TOWNSTAR + ETH
    --   and tx_hash = '\xa9cb862c5a172319dd403f72e5ba806708d7bb8774a2f61d55ea169d27923a19' -- same seller, token_contract, token_id but 4 transfers 
    --   and tx_hash = '\x3c86aa7172ff0971b3d2450f2aa09e2aa99492b774403bfd036113accaccdcc2' -- MEV
    --   and tx_hash = '\x4ac895a0975574084899a93496b08e2d05808d39189d3a27ce8ac3935679aa15' -- 2nft, buy and move, and no price
    --   and tx_hash = '\x287ee2e4dfa7feee4b7c3c54f74013088fd7fe87c382103c45562e050019cc20' -- 3 erc1155, buy, move item so erc20->erc1155 
    --  and tx_hash = '\x698024deef22eace175e3a5fe81ec7219970aa13af1efada75df8813d9c06f4e' -- self wash trade -- very rare and bad case
    --  and tx_hash =  '\x79f2ca6153e03b186cd6ec1d7bc96f50309f0e8a86cc302de48d805d076c3fe2' -- buy 1 simul and send them all to another wallet
    --  and tx_hash = '\x5022f60c36cb85b777f4e0ce5768465234b496030a1909d17ab5a6b8f514d759' -- buy 3 simul and send them all to another wallet
     group by 1,2,3
)
-- select *
--   from iv_volume
--  order by 
--  block_time, tx_hash, evt_index 
,iv_nfts as (
    select a.block_time
          ,a.tx_hash
          ,a.evt_index
          ,a.sender as seller
          ,a.receiver as buyer
          ,case when nft_cnt > 1 then 'bundle trade' 
                else 'single trade'
           end as trade_type
          ,a.order_type
          ,a.token_contract_address as nft_contract_address
          ,a.original_amount as nft_token_amount
          ,a.token_id as nft_token_id
          ,a.item_type as nft_token_standard
          ,a.zone
          ,a.exchange_contract_address
          ,b.token_contract_address 
          ,price_amount_raw / nft_cnt as price_amount_raw
          ,platform_fee_amount_raw / nft_cnt as platform_fee_amount_raw
          ,platform_fee_receiver
          ,creator_fee_amount_raw / nft_cnt as creator_fee_amount_raw
          ,creator_fee_amount_1_raw / nft_cnt as creator_fee_amount_1_raw
          ,creator_fee_amount_2_raw / nft_cnt as creator_fee_amount_2_raw
          ,creator_fee_amount_3_raw / nft_cnt as creator_fee_amount_3_raw
          ,creator_fee_amount_4_raw / nft_cnt as creator_fee_amount_4_raw
          ,creator_fee_receiver_1_raw
          ,creator_fee_receiver_2_raw
          ,creator_fee_receiver_3_raw
          ,creator_fee_receiver_4_raw
          ,case when nft_cnt > 1 then true
                else false
           end as estimated_price
      from dune_user_generated.sohwak_os_s10_base_pair_3 a
           left join iv_volume b on b.block_time = a.block_time  -- tx_hash and evt_index is PK, but for performance, block_time is included
                                 and b.tx_hash = a.tx_hash
                                 and b.evt_index = a.evt_index
     where 1=1
       and a.is_traded_nft
    --   and a.tx_hash = '\xa9cb862c5a172319dd403f72e5ba806708d7bb8774a2f61d55ea169d27923a19' -- multi buy
    --   and a.tx_hash = '\x067494c3c2e1dfc68d4806e51ebb76d89f7be369f752fcaa1df4e7d91acbbda8' -- bundle trade
    --   and a.tx_hash = '\x08eba6fac7c403068b8ed13bd2fc37e9717a29929581a66c9e88ca88fa206a3d' -- multitrade, different zone so different platform fees
    --   and a.tx_hash = '\x0001e6fd67badfda25f85364215572173b1eff29ff11362987562b23b730fb47' -- offer_accepted, 1 nft
    --   and a.tx_hash = '\x8cd79572bb14ee10a07a149644b4e5f37f62cb5d029ea23713107094f53ff68c' -- offer_accepted, 20 nft, weth
    --   and a.tx_hash = '\x0b836ca871e15048c48c4143c243d7c7f35323fd20ecd394ce004fe48101edc6' -- offer_accepted, 2x creator fee 
    --   and a.tx_hash = '\xd95bfa3251c5971ca7dec1b1cbde9ffb8637074810fafbdda4cfacf32e2698c9' -- TOWNSTAR + ETH
    --   and a.tx_hash = '\xa9cb862c5a172319dd403f72e5ba806708d7bb8774a2f61d55ea169d27923a19' -- same seller, token_contract, token_id but 4 transfers 
    --   and a.tx_hash = '\x3c86aa7172ff0971b3d2450f2aa09e2aa99492b774403bfd036113accaccdcc2' -- MEV arbitrage, but self?
    --   and a.tx_hash = '\x4ac895a0975574084899a93496b08e2d05808d39189d3a27ce8ac3935679aa15' -- 2nft, buy and move, and no price
    --   and a.tx_hash = '\x287ee2e4dfa7feee4b7c3c54f74013088fd7fe87c382103c45562e050019cc20' -- 3 erc1155, buy, move item so erc20->erc1155 
    --  and a.tx_hash = '\x698024deef22eace175e3a5fe81ec7219970aa13af1efada75df8813d9c06f4e' -- self wash trade -- very rare and bad case
    --  and a.tx_hash =  '\x79f2ca6153e03b186cd6ec1d7bc96f50309f0e8a86cc302de48d805d076c3fe2' -- buy 1 simul and send them all to another wallet
    --  and a.tx_hash = '\x5022f60c36cb85b777f4e0ce5768465234b496030a1909d17ab5a6b8f514d759' -- buy 3 simul and send them all to another wallet
    --  and a.tx_hash = '\xe1efb363d1d6a726c033d902da2e9ed3330486e9544086bb24c5315d5660b2f0' -- private1
    --  and a.tx_hash = '\xe1efb363d1d6a726c033d902da2e9ed3330486e9544086bb24c5315d5660b2f0' -- private2 -- no cost
    --  and a.tx_hash = '\x69a51f105786c9aa4afa15fcb91148c52e97cd1a3a87065c0d5b7b7f3f8eae1c' -- private3
    --  group by 1,2,3
)
select 
    --   sum(price_amount_raw)
    --   ,sum(platform_fee_amount_raw)
       *
  from iv_nfts
 limit 1000


-- select block_time
--       ,tx_hash
--       ,evt_index
--       ,token_contract_address
--       ,
--   from dune_user_generated.sohwak_os_s10_base_pair a
--  where 1=1
--   and is_traded_nft 
--   and tx_hash = '\xa9cb862c5a172319dd403f72e5ba806708d7bb8774a2f61d55ea169d27923a19' -- multi buy
   
   